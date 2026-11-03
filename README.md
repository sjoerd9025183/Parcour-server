# PARKOUR RUSH ONLINE - server

Node.js (>= 18, getest met 22), geen npm-dependencies. Bevat: HTTP-API, eigen WebSocket-implementatie (RFC 6455), kamerbeheer, race-logica en anti-cheat-plausibiliteitscontroles.

    npm start                       # start op poort 8080 (of $PORT)
    curl http://localhost:8080/health
    npm test                        # 39 integratietests (start zelf een server op een vrije poort)

## API
- `POST /api/rooms` `{name}` -> `{code, playerId, token, wsPath, ...}`
- `POST /api/rooms/join` `{code, name}` -> idem, of `{error:{code,message}}` (`unknown_code`, `room_full`, `room_closed`, `rate_limited`)
- `GET /health`
- WebSocket `/ws?token=<token>`; berichten zijn JSON met `v:1` en `t:<type>` (zie `server.js`, functie `onMessage`, en `../docs/HANDLEIDING.md`).

## Waarom deze keuze
Node.js zonder dependencies: een enkel bestand, geen build-stap, draait op elk platform met WebSocket-ondersteuning, en `levels.json` wordt gedeeld met de iPad-app (gegenereerd door `../tools/gen_levels.py`).

## Beperkingen (eerlijk)
- Kamers staan in het geheugen: herstart = alle kamers weg. Geen database/permanente statistieken.
- Eén serverinstantie (geen horizontale schaling).
- De server simuleert geen botsingen; hij controleert snelheid, bereik, checkpointvolgorde en minimale tijd. Een slimme valsspeler die binnen de snelheidslimiet door muren vliegt kan niet volledig worden uitgesloten.
