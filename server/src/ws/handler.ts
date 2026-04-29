/**
 * WebSocket handler — implements y-protocols sync + awareness.
 *
 * y-protocols message format:
 *   Byte 0: message type (0=sync, 1=awareness)
 *   Byte 1+: payload (sync step 1/2/update, or awareness update)
 *
 * Sync handshake flow:
 *   Client → Server: sync-step-1 (client's state vector)
 *   Server → Client: sync-step-2 (missing updates) + sync-step-1
 *   Client → Server: sync-step-2 (server's missing updates)
 *   Then: bidirectional update stream
 */
import * as Y           from 'yjs';
import * as encoding    from 'lib0/encoding';
import * as decoding    from 'lib0/decoding';
import * as syncProtocol      from 'y-protocols/sync';
import * as awarenessProtocol from 'y-protocols/awareness';
import { WebSocket, WebSocketServer } from 'ws';
import { IncomingMessage }  from 'http';
import { v4 as uuid }       from 'uuid';
import { docManager }       from '../crdt/YDocManager';
import { verifyToken }      from '../middleware/auth';

const MESSAGE_SYNC       = 0;
const MESSAGE_AWARENESS  = 1;

export function setupWebSocketServer(wss: WebSocketServer) {
  wss.on('connection', async (ws: WebSocket, req: IncomingMessage) => {
    // ── Parse URL: /ws/:docId?token=... ───────────────
    const url    = new URL(req.url!, `ws://localhost`);
    const parts  = url.pathname.split('/').filter(Boolean);
    const docId  = parts[1];   // /ws/{docId}
    const token  = url.searchParams.get('token');

    if (!docId) {
      ws.close(4001, 'Missing docId');
      return;
    }

    // ── Authenticate ──────────────────────────────────
    let userId = 'anonymous';
    let userName = 'Anonymous';
    let userColor = '#6366f1';
    if (token) {
      try {
        const payload = verifyToken(token);
        userId    = payload.userId;
        userName  = payload.name;
        userColor = payload.color;
      } catch {
        ws.close(4003, 'Invalid token');
        return;
      }
    }

    const clientId = uuid();

    // ── Load/create doc room ──────────────────────────
    const room = await docManager.getOrCreateRoom(docId);
    room.clients.set(clientId, { ws, clientId, userId, docId });

    console.log(`[WS] Client ${clientId} (${userName}) joined doc ${docId}. Active: ${room.clients.size}`);

    // ── Send Sync Step 1: our state vector ────────────
    {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, MESSAGE_SYNC);
      syncProtocol.writeSyncStep1(encoder, room.doc);
      ws.send(encoding.toUint8Array(encoder));
    }

    // ── Send current awareness states ─────────────────
    const awarenessStates = room.awareness.getStates();
    if (awarenessStates.size > 0) {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, MESSAGE_AWARENESS);
      encoding.writeVarUint8Array(
        encoder,
        awarenessProtocol.encodeAwarenessUpdate(
          room.awareness,
          Array.from(awarenessStates.keys())
        )
      );
      ws.send(encoding.toUint8Array(encoder));
    }

    // ── Message handler ───────────────────────────────
    ws.on('message', (rawData: Buffer) => {
      try {
        const decoder = decoding.createDecoder(new Uint8Array(rawData));
        const msgType = decoding.readVarUint(decoder);

        if (msgType === MESSAGE_SYNC) {
          const encoder = encoding.createEncoder();
          encoding.writeVarUint(encoder, MESSAGE_SYNC);

          // syncProtocol.readSyncMessage handles all 3 sync subtypes:
          //   type 0 (step1): writes step2 into encoder
          //   type 1 (step2): applies update to doc
          //   type 2 (update): applies update, broadcasts
          const syncMsgType = syncProtocol.readSyncMessage(
            decoder, encoder, room.doc, null
          );

          // If we produced a response (e.g. step2), send it back
          if (encoding.length(encoder) > 1) {
            ws.send(encoding.toUint8Array(encoder));
          }

          // For update messages, broadcast to all other clients
          if (syncMsgType === syncProtocol.messageYjsSyncStep2 ||
              syncMsgType === syncProtocol.messageYjsUpdate) {
            // already triggered via doc 'update' event in YDocManager
          }

        } else if (msgType === MESSAGE_AWARENESS) {
          const update = decoding.readVarUint8Array(decoder);
          // Apply to our awareness
          awarenessProtocol.applyAwarenessUpdate(
            room.awareness, update, ws
          );
          // Broadcast to everyone else
          docManager.broadcastAwareness(docId, update, clientId);
        }

      } catch (err) {
        console.error('[WS] Message error:', err);
      }
    });

    // ── Cleanup on disconnect ─────────────────────────
    ws.on('close', () => {
      room.clients.delete(clientId);
      // Remove this client's awareness state (cursor disappears)
      awarenessProtocol.removeAwarenessStates(
        room.awareness,
        [room.doc.clientID],
        'disconnect'
      );
      console.log(`[WS] Client ${clientId} left doc ${docId}. Active: ${room.clients.size}`);
    });

    ws.on('error', console.error);
  });
}
