CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE users (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name       TEXT NOT NULL,
  email      TEXT UNIQUE NOT NULL,
  password   TEXT NOT NULL,           -- bcrypt hash
  color      TEXT NOT NULL DEFAULT '#4f46e5',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE documents (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  title      TEXT NOT NULL DEFAULT 'Untitled Document',
  owner_id   UUID REFERENCES users(id) ON DELETE CASCADE,
  -- Yjs state vector snapshot (binary) for fast initial sync
  ydoc_state BYTEA,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE document_access (
  doc_id  UUID REFERENCES documents(id) ON DELETE CASCADE,
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  role    TEXT CHECK (role IN ('viewer', 'editor', 'owner')) DEFAULT 'editor',
  PRIMARY KEY (doc_id, user_id)
);

-- Persist full update history for time-travel / undo
CREATE TABLE ydoc_updates (
  id         BIGSERIAL PRIMARY KEY,
  doc_id     UUID REFERENCES documents(id) ON DELETE CASCADE,
  update     BYTEA NOT NULL,
  client_id  TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ydoc_updates_doc ON ydoc_updates(doc_id, created_at);
