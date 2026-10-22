# Pulsehub

**Realtime WebSocket room hub in Go** — presence, chat history, multi-room, embedded demo client.

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

## Features

- Rooms via query (`?room=lobby&name=pilot`)
- Presence join/leave broadcasts
- Per-room chat history (last 100)
- Ping/pong keepalive
- `/v1/stats` JSON
- Embedded browser client

## Quick start

```bash
go mod tidy
go run .
# open http://localhost:8090
```

```js
const ws = new WebSocket('ws://localhost:8090/ws?name=kian&room=ops')
ws.send(JSON.stringify({ type: 'chat', text: 'hello' }))
```

## License

MIT © [kianPk](https://github.com/kianPk)
