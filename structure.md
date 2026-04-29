┌─────────────────────────────────────────────────────────┐
│                     CLIENTS (React)                     │
│  Slate Editor ←→ Yjs Doc ←→ y-websocket Provider       │
│  Cursor Overlay ←→ Awareness Protocol                   │
└──────────────────────┬──────────────────────────────────┘
                       │ WebSocket (ws://)
┌──────────────────────▼──────────────────────────────────┐
│                  NODE.JS SERVER                         │
│  Express API  ←→  WebSocket Server (ws)                 │
│  Yjs Docs Map ←→  y-protocols (sync + awareness)        │
│       │                                                  │
│  Redis Pub/Sub  ←── horizontal scale across instances   │
│  PostgreSQL     ←── doc snapshots + user auth           │
└─────────────────────────────────────────────────────────┘
