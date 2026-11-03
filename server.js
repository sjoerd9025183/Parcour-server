// PARKOUR RUSH ONLINE - multiplayer server
// Node.js >= 18, geen externe dependencies (eigen minimale RFC 6455 WebSocket-implementatie).
// HTTP: POST /api/rooms, POST /api/rooms/join, GET /health   |   WebSocket: /ws?token=...
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const PROTOCOL_VERSION = 1;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MAX_PLAYERS = 8;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
export const MUTATIONS = {   // speed/jump = factor voor de plausibiliteitscontroles; de client voert het effect zelf uit
  lowGravity: { name: 'Low Gravity', desc: 'Je springt hoger en valt langzamer.', speed: 1, jump: 1.2 },
  highSpeed: { name: 'High Speed', desc: 'Iedereen beweegt 40% sneller.', speed: 1.4 },
  noSprint: { name: 'No Sprint', desc: 'Sprinten is uitgeschakeld (lopen is wel iets sneller).' },
  slippery: { name: 'Slippery Floor', desc: 'Oppervlakken zijn glad: remmen en keren kost tijd.' },
  doubleJump: { name: 'Double Jump', desc: 'Je mag één keer extra springen in de lucht.', jump: 1.1 },
  bigJumps: { name: 'Big Jumps', desc: 'Sprongen krijgen meer hoogte en bereik.', jump: 1.3 },
  fog: { name: 'Fog Mode', desc: 'Dikke mist: beperkt zicht.' },
  reverse: { name: 'Reverse Controls', desc: 'Op vaste momenten is de besturing even omgekeerd.' },
  oneLife: { name: 'One Life', desc: 'Eén leven: wie valt is uit de ronde.' },
  movingPlatforms: { name: 'Moving Platforms', desc: 'Verdwijnende en bewegende platforms gaan sneller.', speed: 1.15 },
};
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

export function createServer(opts = {}) {
  const cfg = {
    port: Number(opts.port ?? process.env.PORT ?? 8080),
    host: opts.host ?? process.env.HOST ?? '0.0.0.0',
    maxRooms: Number(opts.maxRooms ?? process.env.MAX_ROOMS ?? 500),
    trustProxy: (opts.trustProxy ?? process.env.TRUST_PROXY ?? '0') === '1' || opts.trustProxy === true,
    countdownMs: Number(opts.countdownMs ?? process.env.COUNTDOWN_MS ?? 3500),
    graceMs: Number(opts.graceMs ?? process.env.RECONNECT_GRACE_MS ?? 45000),
    idleRoomMs: Number(opts.idleRoomMs ?? process.env.IDLE_ROOM_MS ?? 30 * 60 * 1000),
    heartbeatTimeoutMs: Number(opts.heartbeatTimeoutMs ?? process.env.HEARTBEAT_TIMEOUT_MS ?? 15000),
    snapshotHz: Number(opts.snapshotHz ?? process.env.SNAPSHOT_HZ ?? 15),
    levelsPath: opts.levelsPath ?? process.env.LEVELS_PATH ?? path.join(__dirname, 'levels.json'),
    quiet: opts.quiet ?? false,
    noRateLimit: opts.noRateLimit ?? false,
    voteMs: Number(opts.voteMs ?? process.env.VOTE_MS ?? 20000),
  };
  const LIM = { // plausibiliteitsgrenzen (client-constanten: sprint 8.5 m/s, spring 8.6 m/s, g 24)
    maxHSpeed: 15.0, maxUp: 13.0, maxDown: 55.0, slack: 1.6, maxStrikes: 25,
    msgPerSec: 60, maxMsgBytes: 2048, maxBody: 1024,
  };
  const levelsFile = JSON.parse(fs.readFileSync(cfg.levelsPath, 'utf8'));
  const levels = new Map(levelsFile.levels.map(l => [l.id, l]));
  const defaultMap = levelsFile.levels[0].id;
  for (const l of levels.values()) {            // minimale tijd (s) tot elk checkpoint (rechte lijn / 12 m/s)
    let prev = l.spawn.p, acc = 0; l.minTimes = [];
    for (const c of l.checkpoints) { acc += dist3(prev, c.p); l.minTimes.push(acc / 12); prev = c.p; }
    acc += dist3(prev, l.finish.p); l.minFinish = acc / 12;
  }

  const rooms = new Map();      // code -> room
  const byToken = new Map();    // token -> {room, player}
  const ipBuckets = new Map();  // ip -> {create:[], join:[], conns:n}
  const log = (...a) => { if (!cfg.quiet) console.log(new Date().toISOString(), ...a); };

  // ------------------------------------------------------------------ helpers
  function dist3(a, b) { return Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]); }
  function dist2(a, b) { return Math.hypot(a[0] - b[0], a[2] - b[2]); }
  const now = () => Date.now();
  const rndInt = n => crypto.randomInt(n);
  function newCode() {
    for (let i = 0; i < 50; i++) {
      let s = ''; for (let k = 0; k < 4; k++) s += CODE_ALPHABET[rndInt(CODE_ALPHABET.length)];
      const code = 'RUSH-' + s;
      if (!rooms.has(code)) return code;        // unieke code, anders opnieuw genereren
    }
    return null;
  }
  const newId = () => crypto.randomBytes(6).toString('hex');
  const newToken = () => crypto.randomBytes(24).toString('base64url');
  function cleanName(n) {
    let s = String(n ?? '').replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 16);
    return s.length ? s : 'Runner-' + (100 + rndInt(900));
  }
  function normCode(c) { return String(c ?? '').toUpperCase().replace(/\s+/g, '').slice(0, 12); }
  function bucket(ip) {
    let b = ipBuckets.get(ip); if (!b) { b = { create: [], join: [], conns: 0 }; ipBuckets.set(ip, b); } return b;
  }
  function rate(arr, limit, windowMs) {
    if (cfg.noRateLimit) return true;
    const t = now(); while (arr.length && t - arr[0] > windowMs) arr.shift();
    if (arr.length >= limit) return false; arr.push(t); return true;
  }
  const finite3 = v => Array.isArray(v) && v.length === 3 && v.every(Number.isFinite);

  // ------------------------------------------------------------------ rooms
  function publicPlayer(r, p) {
    return { id: p.id, name: p.name, ready: p.ready, connected: p.connected, host: r.hostId === p.id,
      cp: p.cp, finished: p.finished, time: p.finishMs, penalty: p.penaltyMs, respawns: p.respawns, dq: p.dq };
  }
  function roomState(r, forPlayer) {
    const lvl = levels.get(r.map);
    return { v: PROTOCOL_VERSION, t: 'state', serverTime: now(), you: forPlayer?.id ?? null,
      room: { code: r.code, id: r.id, status: r.status, map: r.map, hostId: r.hostId, max: MAX_PLAYERS, cpTotal: lvl.checkpoints.length,
        players: [...r.players.values()].map(p => publicPlayer(r, p)) },
      mutation: r.mutation ? { id: r.mutation, ...MUTATIONS[r.mutation] } : null,
      vote: voteView(r, forPlayer),
      race: r.raceId ? { id: r.raceId, goAt: r.goAt, spawnIdx: forPlayer?.spawnIdx ?? 0, timeLimit: lvl.timeLimit } : null,
      me: forPlayer ? { cp: forPlayer.cp, respawns: forPlayer.respawns, finished: forPlayer.finished, time: forPlayer.finishMs } : null,
      results: r.status === 'finished' ? r.results : null };
  }
  function voteView(r, p) {
    const v = r.vote; if (!v) return null;
    const count = (opts, votes) => Object.fromEntries(opts.map(o => [o, [...votes.values()].filter(x => x === o).length]));
    return { id: v.id, endsAt: v.endsAt, serverTime: now(), maps: v.maps.map(id => id === 'random' ? { id, name: 'Willekeurige map' } : { id, name: levels.get(id).name }),
      mutations: v.muts.map(id => id === 'random' ? { id, name: 'Willekeurig', desc: 'De server kiest een willekeurige mutatie.' } : { id, ...MUTATIONS[id] }),
      mapCounts: count(v.maps, v.mapVotes), mutCounts: count(v.muts, v.mutVotes),
      mine: p ? { map: v.mapVotes.get(p.id) ?? null, mutation: v.mutVotes.get(p.id) ?? null } : null, voters: v.mapVotes.size + v.mutVotes.size };
  }
  function shuffle(a) { const b = [...a]; for (let i = b.length - 1; i > 0; i--) { const j = rndInt(i + 1); [b[i], b[j]] = [b[j], b[i]]; } return b; }
  function startVote(r) {
    r.vote = { id: newId(), endsAt: now() + cfg.voteMs, maps: [...shuffle([...levels.keys()]), 'random'],
      muts: [...shuffle(Object.keys(MUTATIONS)).slice(0, 3), 'random'], mapVotes: new Map(), mutVotes: new Map(), timer: null };
    r.vote.timer = setTimeout(() => resolveVote(r), cfg.voteMs); r.timers.push(r.vote.timer);
    broadcast(r, { t: 'voteStarted', endsAt: r.vote.endsAt }); broadcastState(r);
  }
  function pickWinner(options, votes) {
    const real = options.filter(o => o !== 'random');
    const counts = options.map(o => [o, [...votes.values()].filter(x => x === o).length]);
    const max = Math.max(...counts.map(c => c[1]));
    let w = max === 0 ? real[rndInt(real.length)] : (() => { const top = counts.filter(c => c[1] === max).map(c => c[0]); return top[rndInt(top.length)]; })();
    if (w === 'random') w = real[rndInt(real.length)];
    return w;
  }
  function resolveVote(r) {
    const v = r.vote; if (!v) return;
    clearTimeout(v.timer); r.timers = r.timers.filter(x => x !== v.timer);
    r.map = pickWinner(v.maps, v.mapVotes); r.mutation = pickWinner(v.muts, v.mutVotes);
    r.vote = null; for (const x of r.players.values()) x.ready = false;
    broadcast(r, { t: 'voteResult', map: r.map, mutation: { id: r.mutation, ...MUTATIONS[r.mutation] } }); broadcastState(r);
  }
  function send(p, obj) { if (p.conn && p.connected) p.conn.sendJson({ v: PROTOCOL_VERSION, ...obj }); }
  function broadcast(r, obj, exceptId) { for (const p of r.players.values()) if (p.id !== exceptId) send(p, obj); }
  function broadcastState(r) { for (const p of r.players.values()) send(p, roomState(r, p)); }
  const connectedPlayers = r => [...r.players.values()].filter(p => p.connected);

  function makePlayer(r, name) {
    const p = { id: newId(), token: newToken(), name: cleanName(name), ready: false, connected: false, conn: null,
      lastSeen: now(), disconnectedAt: now(), cp: 0, finished: false, finishMs: null, penaltyMs: 0, respawns: 0, dq: false, out: false,
      pos: null, yaw: 0, pitch: 0, flags: 0, lastPosAt: 0, strikes: 0, strikeWindow: 0, spawnIdx: 0, msgTokens: LIM.msgPerSec, msgAt: now(), lastRespawnAt: 0, joinedAt: now() };
    r.players.set(p.id, p); byToken.set(p.token, { room: r, player: p });
    return p;
  }
  function createRoom(name) {
    if (rooms.size >= cfg.maxRooms) return { error: { code: 'server_busy', message: 'Server is vol, probeer het later opnieuw.' } };
    const code = newCode(); if (!code) return { error: { code: 'server_busy', message: 'Kon geen kamercode maken.' } };
    const r = { id: newId(), code, status: 'lobby', map: defaultMap, hostId: null, players: new Map(), raceId: null, goAt: 0,
      timers: [], snapshotTimer: null, results: null, mutation: null, vote: null, createdAt: now(), lastActive: now() };
    rooms.set(code, r);
    const p = makePlayer(r, name); r.hostId = p.id;
    log('room created', code); return { room: r, player: p };
  }
  function joinRoom(code, name) {   // synchroon => atomair (single-threaded event loop)
    const r = rooms.get(normCode(code));
    if (!r) return { error: { code: 'unknown_code', message: 'Onbekende kamercode.' } };
    if (r.status === 'closed') return { error: { code: 'room_closed', message: 'Deze kamer is gesloten.' } };
    if (r.status !== 'lobby') return { error: { code: 'room_closed', message: 'De race in deze kamer is al gestart.' } };
    if (r.players.size >= MAX_PLAYERS) return { error: { code: 'room_full', message: 'Deze kamer is vol (8/8).' } };
    const p = makePlayer(r, name); r.lastActive = now(); log('join', r.code, p.name, r.players.size);
    return { room: r, player: p };
  }
  function removePlayer(r, p, reason) {
    if (!r.players.has(p.id)) return;
    r.players.delete(p.id); byToken.delete(p.token);
    if (p.conn) { try { p.conn.close(1000, 'left'); } catch {} p.conn = null; p.connected = false; }
    r.lastActive = now();
    if (r.players.size === 0) return destroyRoom(r, 'empty');
    if (r.hostId === p.id) {
      const next = connectedPlayers(r).sort((a, b) => a.joinedAt - b.joinedAt)[0];
      if (!next) { /* niemand verbonden: kamer blijft tot grace voorbij is */ r.hostId = [...r.players.values()][0].id; }
      else r.hostId = next.id;
    }
    broadcast(r, { t: 'playerLeft', id: p.id, name: p.name, reason, hostId: r.hostId });
    broadcastState(r);
    if (r.status === 'racing') maybeEndRace(r);
  }
  function destroyRoom(r, reason) {
    r.timers.forEach(clearTimeout); r.timers = []; clearInterval(r.snapshotTimer);
    for (const p of r.players.values()) { byToken.delete(p.token); if (p.conn) { try { p.conn.close(1000, reason); } catch {} } }
    r.status = 'closed'; rooms.delete(r.code); log('room closed', r.code, reason);
  }
  const setTimer = (r, ms, fn) => { const t = setTimeout(() => { r.timers = r.timers.filter(x => x !== t); fn(); }, ms); r.timers.push(t); };

  // ------------------------------------------------------------------ race
  function startRace(r) {
    const lvl = levels.get(r.map);
    r.status = 'countdown'; r.raceId = newId(); r.goAt = now() + cfg.countdownMs; r.results = null;
    let i = 0;
    for (const p of [...r.players.values()].sort((a, b) => a.joinedAt - b.joinedAt)) {
      p.spawnIdx = i++; p.cp = 0; p.finished = false; p.finishMs = null; p.penaltyMs = 0; p.respawns = 0; p.dq = false; p.out = false; p.strikes = 0;
      const slot = lvl.spawn.slots[p.spawnIdx % 8];
      p.pos = [lvl.spawn.p[0] + slot[0], lvl.spawn.p[1], lvl.spawn.p[2] + slot[1]]; p.lastPosAt = 0; p.flags = 0;
    }
    for (const p of r.players.values()) send(p, { t: 'countdown', raceId: r.raceId, goAt: r.goAt, serverTime: now(), map: r.map,
      spawn: p.pos, yaw: lvl.spawn.yaw, spawnIdx: p.spawnIdx, timeLimit: lvl.timeLimit, mutation: r.mutation });
    broadcastState(r);
    setTimer(r, Math.max(0, r.goAt - now()), () => {
      if (r.status !== 'countdown') return;
      r.status = 'racing'; broadcast(r, { t: 'raceStart', raceId: r.raceId, goAt: r.goAt, serverTime: now() });
      for (const p of r.players.values()) p.lastPosAt = now();
      r.snapshotTimer = setInterval(() => sendSnapshots(r), Math.round(1000 / cfg.snapshotHz));
      setTimer(r, lvl.timeLimit * 1000, () => endRace(r, 'timeout'));
    });
  }
  function sendSnapshots(r) {
    const list = [];
    for (const p of r.players.values()) if (p.pos) list.push({ id: p.id, p: p.pos.map(v => Math.round(v * 100) / 100), y: Math.round(p.yaw * 100) / 100, pt: Math.round(p.pitch * 100) / 100, f: p.flags, th: Math.round((p.th ?? 0) * 100) / 100, c: p.connected ? 1 : 0 });
    for (const p of r.players.values()) if (p.connected && p.conn.socket.writableLength < 262144) send(p, { t: 'snap', n: now(), players: list });
  }
  function maybeEndRace(r) {
    if (r.status !== 'racing') return;
    const active = [...r.players.values()].filter(p => !p.finished && !p.dq && !p.out);
    if (active.length === 0) endRace(r, 'all_finished');
  }
  function endRace(r, reason) {
    if (r.status !== 'racing' && r.status !== 'countdown') return;
    r.timers.forEach(clearTimeout); r.timers = []; clearInterval(r.snapshotTimer);
    const lvl = levels.get(r.map);
    const rows = [...r.players.values()].map(p => ({ id: p.id, name: p.name, checkpoints: p.cp, cpTotal: lvl.checkpoints.length,
      penalty: p.penaltyMs, time: p.finishMs, finalTime: p.finished ? p.finishMs + p.penaltyMs : null,
      status: p.dq ? 'DQ' : p.out ? 'ELIMINATED' : p.finished ? 'FINISHED' : 'DNF' }));
    rows.sort((a, b) => (a.finalTime != null && b.finalTime != null) ? a.finalTime - b.finalTime
      : a.finalTime != null ? -1 : b.finalTime != null ? 1 : b.checkpoints - a.checkpoints);
    rows.forEach((x, i) => x.rank = i + 1);
    r.results = rows; r.status = 'finished';
    for (const p of r.players.values()) p.ready = false;
    broadcast(r, { t: 'raceEnd', raceId: r.raceId, reason, results: rows });
    broadcastState(r); log('race end', r.code, reason);
  }
  function strike(r, p, why, ws) {
    p.strikes++;
    send(p, { t: 'correct', p: p.pos, yaw: p.yaw, reason: why });
    if (p.strikes >= LIM.maxStrikes && !p.dq) { p.dq = true; send(p, { t: 'error', code: 'disqualified', message: 'Gediskwalificeerd: onmogelijke bewegingen.' }); maybeEndRace(r); }
  }
  function handlePos(r, p, m) {
    if (r.status !== 'racing' || p.finished || p.dq || p.out) return;
    if (!finite3(m.p)) return;
    const lvl = levels.get(r.map), b = lvl.bounds, t = now();
    for (let i = 0; i < 3; i++) if (m.p[i] < b.min[i] || m.p[i] > b.max[i]) return strike(r, p, 'bounds');
    const dt = Math.min(2.0, Math.max(0.03, (t - p.lastPosAt) / 1000));
    const mu = MUTATIONS[r.mutation] ?? {}; const sp = mu.speed ?? 1, jm = mu.jump ?? 1;
    const hd = dist2(m.p, p.pos), dy = m.p[1] - p.pos[1];
    if (hd > LIM.maxHSpeed * sp * dt + LIM.slack) return strike(r, p, 'speed');
    if (dy > LIM.maxUp * jm * dt + LIM.slack) return strike(r, p, 'speed_up');
    if (-dy > LIM.maxDown * dt + LIM.slack + 3) return strike(r, p, 'speed_down');
    p.pos = m.p.map(Number); p.lastPosAt = t;
    p.yaw = Number.isFinite(m.yaw) ? m.yaw : p.yaw; p.pitch = Number.isFinite(m.pitch) ? Math.max(-1.6, Math.min(1.6, m.pitch)) : p.pitch;
    p.flags = Number.isInteger(m.f) ? (m.f & 0xff) : 0;
    p.th = Number.isFinite(m.th) ? Math.max(-1.6, Math.min(1.6, m.th)) : 0;     // slingerhoek / muurkant, alleen voor animatie bij anderen
    const elapsed = (t - r.goAt) / 1000;
    if (p.cp < lvl.checkpoints.length) {                 // volgende checkpoint in vaste volgorde
      const c = lvl.checkpoints[p.cp];
      if (dist2(p.pos, c.p) <= c.r && Math.abs(p.pos[1] - c.p[1]) <= 3.0) {
        if (elapsed >= lvl.minTimes[p.cp] / sp) { p.cp++; send(p, { t: 'checkpoint', id: p.id, index: p.cp, total: lvl.checkpoints.length, time: Math.round(elapsed * 1000) });
          broadcast(r, { t: 'playerUpdate', id: p.id, cp: p.cp }, p.id); }
        else strike(r, p, 'too_fast');
      }
    } else if (!p.finished) {                            // alle checkpoints gehaald -> finish
      if (dist2(p.pos, lvl.finish.p) <= lvl.finish.r && Math.abs(p.pos[1] - lvl.finish.p[1]) <= 3.0 && elapsed >= lvl.minFinish / sp) {
        p.finished = true; p.finishMs = Math.round(elapsed * 1000);
        const rank = [...r.players.values()].filter(x => x.finished).length;
        broadcast(r, { t: 'finished', id: p.id, name: p.name, time: p.finishMs, penalty: p.penaltyMs, rank });
        broadcastState(r); maybeEndRace(r);
      }
    }
  }
  function handleRespawn(r, p) {
    if (r.status !== 'racing' || p.finished || p.dq || p.out) return;
    if (r.mutation === 'oneLife') { p.out = true; send(p, { t: 'eliminated', id: p.id }); broadcast(r, { t: 'playerUpdate', id: p.id, out: true }, p.id); return maybeEndRace(r); }
    const t = now(); if (t - p.lastRespawnAt < 700) return; p.lastRespawnAt = t;
    const lvl = levels.get(r.map);
    const base = p.cp > 0 ? lvl.checkpoints[p.cp - 1].p : (() => { const s = lvl.spawn.slots[p.spawnIdx % 8]; return [lvl.spawn.p[0] + s[0], lvl.spawn.p[1], lvl.spawn.p[2] + s[1]]; })();
    p.pos = [...base]; p.lastPosAt = t; p.respawns++; p.penaltyMs += 1000;   // 1 s straftijd per val
    send(p, { t: 'respawned', p: p.pos, cp: p.cp, respawns: p.respawns, penalty: p.penaltyMs });
  }

  // ------------------------------------------------------------------ websocket messages
  function onMessage(conn, text) {
    const ctx = conn.ctx; if (!ctx) return; const { room: r, player: p } = ctx;
    const t = now(); p.msgTokens = Math.min(LIM.msgPerSec, p.msgTokens + (t - p.msgAt) / 1000 * LIM.msgPerSec); p.msgAt = t;
    if (p.msgTokens < 1) return; p.msgTokens -= 1;
    let m; try { m = JSON.parse(text); } catch { return send(p, { t: 'error', code: 'bad_json', message: 'Ongeldig bericht.' }); }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
    p.lastSeen = t; r.lastActive = t;
    switch (m.t) {
      case 'ping': return send(p, { t: 'pong', c: m.c, s: t });
      case 'pos': return handlePos(r, p, { p: m.p, yaw: m.y, pitch: m.pt, f: m.f, th: m.th });
      case 'respawn': return handleRespawn(r, p);
      case 'ready':
        if (r.status !== 'lobby') return; p.ready = !!m.ready; broadcast(r, { t: 'playerUpdate', id: p.id, ready: p.ready }); return broadcastState(r);
      case 'setName': p.name = cleanName(m.name); return broadcastState(r);
      case 'startVote':
        if (r.hostId !== p.id) return send(p, { t: 'error', code: 'not_host', message: 'Alleen de host kan een stemronde starten.' });
        if (r.status !== 'lobby' || r.vote) return send(p, { t: 'error', code: 'bad_state', message: 'Er kan nu geen stemronde starten.' });
        return startVote(r);
      case 'vote': {
        const v = r.vote; if (!v || r.status !== 'lobby') return send(p, { t: 'error', code: 'no_vote', message: 'Er is geen stemronde actief.' });
        const kind = m.kind === 'map' ? 'map' : m.kind === 'mutation' ? 'mutation' : null; if (!kind) return;
        const opts2 = kind === 'map' ? v.maps : v.muts; if (typeof m.option !== 'string' || !opts2.includes(m.option)) return send(p, { t: 'error', code: 'bad_option', message: 'Ongeldige keuze.' });
        (kind === 'map' ? v.mapVotes : v.mutVotes).set(p.id, m.option);     // 1 stem per speler, aanpasbaar zolang de timer loopt
        broadcastState(r);
        const need = connectedPlayers(r).length; const done = connectedPlayers(r).every(x => v.mapVotes.has(x.id) && v.mutVotes.has(x.id));
        if (done && need > 0) resolveVote(r); return;
      }
      case 'setMap':
        if (r.hostId !== p.id) return send(p, { t: 'error', code: 'not_host', message: 'Alleen de host kan de map kiezen.' });
        if (r.status !== 'lobby' || r.vote || !levels.has(m.map)) return send(p, { t: 'error', code: 'bad_state', message: 'Map kan nu niet worden gewijzigd.' });
        r.map = m.map; for (const x of r.players.values()) x.ready = false; broadcast(r, { t: 'mapChanged', map: r.map }); return broadcastState(r);
      case 'start': {
        if (r.hostId !== p.id) return send(p, { t: 'error', code: 'not_host', message: 'Alleen de host kan de race starten.' });
        if (r.status !== 'lobby' || r.vote) return send(p, { t: 'error', code: 'bad_state', message: 'De race kan nu niet worden gestart (stemronde actief?).' });
        const notReady = connectedPlayers(r).filter(x => x.id !== r.hostId && !x.ready);
        if (notReady.length) return send(p, { t: 'error', code: 'not_ready', message: `${notReady.length} speler(s) zijn nog niet klaar.` });
        for (const x of [...r.players.values()]) if (!x.connected) removePlayer(r, x, 'disconnected');
        return startRace(r);
      }
      case 'rematch':
        if (r.hostId !== p.id || r.status !== 'finished') return send(p, { t: 'error', code: 'bad_state', message: 'Rematch kan alleen door de host na een race.' });
        r.status = 'lobby'; r.raceId = null; r.results = null; r.mutation = null; r.vote = null; for (const x of r.players.values()) { x.ready = false; x.cp = 0; x.finished = false; }
        return broadcastState(r);
      case 'closeRoom':
        if (r.hostId !== p.id) return send(p, { t: 'error', code: 'not_host', message: 'Alleen de host kan de kamer sluiten.' });
        broadcast(r, { t: 'roomClosed', reason: 'host_closed' }); return destroyRoom(r, 'host_closed');
      case 'leave': return removePlayer(r, p, 'left');
    }
  }
  function attach(conn, token) {
    const ctx = byToken.get(token);
    if (!ctx) { conn.sendJson({ v: PROTOCOL_VERSION, t: 'error', code: 'session_expired', message: 'Sessie verlopen.' }); return conn.close(4401, 'session'); }
    const { room: r, player: p } = ctx;
    if (p.conn && p.conn !== conn) { try { p.conn.sendJson({ v: PROTOCOL_VERSION, t: 'error', code: 'replaced', message: 'Verbonden vanaf een ander apparaat.' }); p.conn.close(4000, 'replaced'); } catch {} p.conn.ctx = null; }
    const wasConnected = p.connected, reconnect = p.pos !== null;
    conn.ctx = ctx; p.conn = conn; p.connected = true; p.lastSeen = now(); r.lastActive = now();
    send(p, roomState(r, p));
    broadcast(r, { t: wasConnected ? 'playerUpdate' : 'playerJoined', id: p.id, name: p.name, connected: true, reconnect }, p.id);
    broadcastState(r);
  }
  function onClose(conn) {
    const ctx = conn.ctx; if (!ctx) return; const { room: r, player: p } = ctx;
    if (p.conn !== conn) return;
    p.connected = false; p.conn = null; p.disconnectedAt = now();
    if (rooms.has(r.code)) { broadcast(r, { t: 'playerUpdate', id: p.id, connected: false }); broadcastState(r); }
  }

  // ------------------------------------------------------------------ http
  function clientIp(req) {
    if (cfg.trustProxy) { const x = req.headers['x-forwarded-for']; if (x) return String(x).split(',')[0].trim(); }
    return req.socket.remoteAddress || 'unknown';
  }
  function json(res, code, obj) { const s = JSON.stringify(obj); res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(s); }
  function readBody(req) {
    return new Promise((resolve, reject) => {
      let size = 0; const chunks = [];
      req.on('data', c => { size += c.length; if (size > LIM.maxBody) { reject(new Error('too_large')); req.destroy(); } else chunks.push(c); });
      req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); } catch { reject(new Error('bad_json')); } });
      req.on('error', reject);
    });
  }
  function sessionReply(r, p) {
    return { v: PROTOCOL_VERSION, code: r.code, roomId: r.id, playerId: p.id, name: p.name, token: p.token, wsPath: '/ws', map: r.map, maps: [...levels.values()].map(l => ({ id: l.id, name: l.name })) };
  }
  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x'); const ip = clientIp(req); const b = bucket(ip);
    try {
      if (req.method === 'GET' && url.pathname === '/health') return json(res, 200, { ok: true, v: PROTOCOL_VERSION, rooms: rooms.size, uptime: Math.round(process.uptime()) });
      if (req.method === 'POST' && url.pathname === '/api/rooms') {
        if (!rate(b.create, 10, 60000)) return json(res, 429, { error: { code: 'rate_limited', message: 'Te veel verzoeken.' } });
        const body = await readBody(req); const out = createRoom(body.name);
        return out.error ? json(res, 503, out) : json(res, 201, sessionReply(out.room, out.player));
      }
      if (req.method === 'POST' && url.pathname === '/api/rooms/join') {
        if (!rate(b.join, 30, 60000)) return json(res, 429, { error: { code: 'rate_limited', message: 'Te veel verzoeken.' } });
        const body = await readBody(req); const out = joinRoom(body.code, body.name);
        return out.error ? json(res, out.error.code === 'unknown_code' ? 404 : 409, out) : json(res, 200, sessionReply(out.room, out.player));
      }
      return json(res, 404, { error: { code: 'not_found', message: 'Niet gevonden.' } });
    } catch (e) {
      return json(res, e.message === 'too_large' ? 413 : 400, { error: { code: e.message || 'bad_request', message: 'Ongeldig verzoek.' } });
    }
  });
  httpServer.on('upgrade', (req, socket) => {
    const url = new URL(req.url, 'http://x'); const ip = clientIp(req); const b = bucket(ip);
    const key = req.headers['sec-websocket-key'];
    if (url.pathname !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || b.conns >= 20) { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    socket.setNoDelay(true); b.conns++;
    const conn = new WSConn(socket, LIM.maxMsgBytes, text => onMessage(conn, text), () => { b.conns = Math.max(0, b.conns - 1); onClose(conn); });
    attach(conn, url.searchParams.get('token') || '');
  });

  // opruimen + heartbeat-timeouts
  const sweeper = setInterval(() => {
    const t = now();
    for (const r of [...rooms.values()]) {
      for (const p of [...r.players.values()]) {
        if (p.connected && t - p.lastSeen > cfg.heartbeatTimeoutMs) { try { p.conn.close(4408, 'timeout'); } catch {} }
        else if (!p.connected && t - p.disconnectedAt > cfg.graceMs) removePlayer(r, p, 'timeout');
      }
      if (rooms.has(r.code) && connectedPlayers(r).length === 0 && t - r.lastActive > cfg.idleRoomMs) destroyRoom(r, 'idle');
    }
  }, 500);
  sweeper.unref?.();

  return {
    cfg, rooms, levels, httpServer,
    listen: () => new Promise(res => httpServer.listen(cfg.port, cfg.host, () => { log(`Parkour Rush server op ${cfg.host}:${httpServer.address().port}`); res(httpServer.address().port); })),
    close: () => { clearInterval(sweeper); for (const r of [...rooms.values()]) destroyRoom(r, 'shutdown'); return new Promise(res => httpServer.close(() => res())); },
  };
}

// ---------------------------------------------------------------------- minimale WebSocket server-connectie
class WSConn {
  constructor(socket, maxBytes, onText, onClosed) {
    this.socket = socket; this.max = maxBytes; this.onText = onText; this.onClosed = onClosed; this.buf = Buffer.alloc(0);
    this.frag = null; this.closed = false; this.ctx = null;
    socket.on('data', d => this.#data(d)); socket.on('close', () => this.#end()); socket.on('error', () => this.#end());
  }
  #end() { if (this.closed) return; this.closed = true; try { this.socket.destroy(); } catch {} this.onClosed(); }
  #data(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > this.max * 4 + 64) return this.close(1009, 'too big');
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0; let len = b[1] & 0x7f, off = 2;
      if (!masked) return this.close(1002, 'unmasked');
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const hi = b.readUInt32BE(2), lo = b.readUInt32BE(6); if (hi !== 0) return this.close(1009, 'too big'); len = lo; off = 10; }
      if (len > this.max) return this.close(1009, 'too big');
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4); const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 0x8) return this.close(1000, 'bye');
      if (op === 0x9) { this.#frame(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x0) {
        if (op === 0x1) this.frag = [];
        if (!this.frag) return this.close(1002, 'protocol');
        this.frag.push(payload);
        if (this.frag.reduce((a, x) => a + x.length, 0) > this.max) return this.close(1009, 'too big');
        if (fin) { const text = Buffer.concat(this.frag).toString('utf8'); this.frag = null; try { this.onText(text); } catch (e) { console.error('handler error', e); } }
      } else if (op === 0x2) { /* binaire frames niet ondersteund: negeren */ }
      else return this.close(1002, 'opcode');
    }
  }
  #frame(op, payload) {
    if (this.closed || this.socket.destroyed) return; const n = payload.length; let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]); else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    this.socket.write(Buffer.concat([h, payload]));
  }
  sendJson(obj) { this.#frame(0x1, Buffer.from(JSON.stringify(obj))); }
  close(code = 1000, reason = '') {
    if (this.closed) return; const rb = Buffer.from(reason).subarray(0, 100); const p = Buffer.alloc(2 + rb.length); p.writeUInt16BE(code, 0); rb.copy(p, 2);
    this.#frame(0x8, p); this.socket.end(); setTimeout(() => this.#end(), 50).unref?.();
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const s = createServer(); s.listen();
  const stop = () => { console.log('Afsluiten...'); s.close().then(() => process.exit(0)); };
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
}
