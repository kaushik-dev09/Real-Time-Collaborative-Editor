/**
 * YDocManager — central registry for all live Yjs documents.
 * Handles: creation, in-memory caching, persistence scheduling,
 *          and broadcast to all WebSocket clients on a doc.
 */
import * as Y from 'yjs';
import * as encoding from 'lib0/encoding';
import * as decoding from 'lib0/decoding';
import * as syncProtocol from 'y-protocols/sync';
import * as awarenessProtocol from 'y-protocols/awareness';
import { WebSocket } from 'ws';
import { db }        from '../db/client';
import { redisPub, redisSub } from '../redis/pubsub';

const PERSISTENCE_DEBOUNCE_MS = 2000;   // save at most every 2s

export interface DocClient {
  ws:        WebSocket;
  clientId:  string;
  userId:    string;
  docId:     string;
}

export interface DocRoom {
  doc:       Y.Doc;
  awareness: awarenessProtocol.Awareness;
  clients:   Map<string, DocClient>;         // clientId → DocClient
  saveTimer: ReturnType<typeof setTimeout> | null;
}

class YDocManager {
  private rooms = new Map<string, DocRoom>();

  // ─────────────────────────────────────────────────
  //  Get or load a document room
  // ─────────────────────────────────────────────────
  async getOrCreateRoom(docId: string): Promise<DocRoom> {
    if (this.rooms.has(docId)) {
      return this.rooms.get(docId)!;
    }

    const doc       = new Y.Doc({ gc: true });
    const awareness = new awarenessProtocol.Awareness(doc);
    const room: DocRoom = { doc, awareness, clients: new Map(), saveTimer: null };

    // ── Load persisted state from PostgreSQL ──────────
    const row = await db.query<{ ydoc_state: Buffer }>(
      'SELECT ydoc_state FROM documents WHERE id = $1',
      [docId]
    );

    if (row.rows[0]?.ydoc_state) {
      // Apply the saved snapshot
      Y.applyUpdate(doc, row.rows[0].ydoc_state);
    }

    // ── Also apply any incremental updates since snapshot ──
    const updates = await db.query<{ update: Buffer }>(
      `SELECT update FROM ydoc_updates
       WHERE doc_id = $1
       ORDER BY created_at ASC`,
      [docId]
    );
    for (const u of updates.rows) {
      Y.applyUpdate(doc, u.update);
    }

    // ── Observe future changes → persist + broadcast ──
    doc.on('update', (update: Uint8Array, origin: unknown) => {
      // Don't re-broadcast updates that came from Redis (already broadcast)
      if (origin !== 'redis') {
        this.onDocUpdate(docId, update, room);
      }
    });

    this.rooms.set(docId, room);

    // ── Subscribe to Redis channel for this doc ───────
    // (other server instances broadcast via Redis)
    await redisSub.subscribe(`doc:${docId}`, (message) => {
      const update = new Uint8Array(JSON.parse(message));
      Y.applyUpdate(doc, update, 'redis');   // mark origin so we don't re-broadcast
      this.broadcastUpdate(docId, update, null);
    });

    console.log(`[YDocManager] Room created: ${docId}`);
    return room;
  }

  // ─────────────────────────────────────────────────
  //  Handle a new update from any client
  // ─────────────────────────────────────────────────
  private onDocUpdate(docId: string, update: Uint8Array, room: DocRoom) {
    // 1. Broadcast to all local clients
    this.broadcastUpdate(docId, update, null);

    // 2. Publish to Redis (other server instances)
    redisPub.publish(`doc:${docId}`, JSON.stringify(Array.from(update)));

    // 3. Schedule debounced persistence
    if (room.saveTimer) clearTimeout(room.saveTimer);
    room.saveTimer = setTimeout(() => this.persistDoc(docId, room), PERSISTENCE_DEBOUNCE_MS);

    // 4. Append to incremental log immediately
    db.query(
      'INSERT INTO ydoc_updates (doc_id, update) VALUES ($1, $2)',
      [docId, Buffer.from(update)]
    ).catch(console.error);
  }

  // ─────────────────────────────────────────────────
  //  Persist a full snapshot (replaces incremental log)
  // ─────────────────────────────────────────────────
  private async persistDoc(docId: string, room: DocRoom) {
    const snapshot = Y.encodeStateAsUpdate(room.doc);
    await db.query(
      `UPDATE documents
       SET ydoc_state = $1, updated_at = NOW()
       WHERE id = $2`,
      [Buffer.from(snapshot), docId]
    );
    // Prune incremental updates older than the snapshot
    await db.query(
      'DELETE FROM ydoc_updates WHERE doc_id = $1',
      [docId]
    );
    console.log(`[YDocManager] Persisted doc: ${docId}`);
  }

  // ─────────────────────────────────────────────────
  //  Broadcast binary message to all clients in room
  // ─────────────────────────────────────────────────
  broadcastUpdate(docId: string, update: Uint8Array, excludeClientId: string | null) {
    const room = this.rooms.get(docId);
    if (!room) return;

    // Wrap in y-protocols sync message (MESSAGE_SYNC = 0, syncUpdate = 2)
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);          // messageSync = 0
    syncProtocol.writeUpdate(encoder, update);
    const msg = encoding.toUint8Array(encoder);

    room.clients.forEach((client, cid) => {
      if (cid === excludeClientId) return;
      if (client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(msg);
      }
    });
  }

  // ─────────────────────────────────────────────────
  //  Broadcast awareness state
  // ─────────────────────────────────────────────────
  broadcastAwareness(docId: string, update: Uint8Array, excludeClientId: string | null) {
    const room = this.rooms.get(docId);
    if (!room) return;

    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 1);          // messageAwareness = 1
    encoding.writeVarUint8Array(encoder, update);
    const msg = encoding.toUint8Array(encoder);

    room.clients.forEach((client, cid) => {
      if (cid === excludeClientId) return;
      if (client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(msg);
      }
    });
  }

  getRoom(docId: string): DocRoom | undefined {
    return this.rooms.get(docId);
  }
}

export const docManager = new YDocManager();
