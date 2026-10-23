# Pulsehub

**Realtime WebSocket room hub in Go** — presence, typing, roster, room switch, optional locked rooms, chat history.

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

## Features

- Rooms via query (`?room=lobby&name=pilot`)
- Presence join/leave + roster snapshot
- Typing indicators and chat rate gap
- In-room switch without reconnect (`type: join`)
- Optional room tokens (`PULSE_ROOM_TOKENS=private:secret`)
- History ring (150) · max room size 64
- `/v1/stats`, `/v1/rooms`, `/v1/presence`
- Embedded browser client + Pages demo

## Quick start

```bash
go mod tidy
go run .
# open http://localhost:8090
```

## Demo

[kianpk.github.io/pulsehub](https://kianpk.github.io/pulsehub/)
