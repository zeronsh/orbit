-- App tables Orbit replicates and serves for the collaborative pixel canvas.
-- better-auth creates its own tables (`pnpm auth:migrate`); Orbit doesn't need
-- to replicate them here.
--
-- REPLICA IDENTITY FULL makes UPDATE/DELETE carry the full old row in the WAL, so
-- Orbit's change feed can apply edits/removes precisely.

CREATE TABLE IF NOT EXISTS pixel (
  id      text    PRIMARY KEY,  -- "x:y"
  x       integer NOT NULL,
  y       integer NOT NULL,
  color   text    NOT NULL,
  updated bigint  NOT NULL
);
ALTER TABLE pixel REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS cursor (
  id      text             PRIMARY KEY,  -- userID
  x       double precision NOT NULL,
  y       double precision NOT NULL,
  color   text             NOT NULL,
  size    integer          NOT NULL,
  erasing integer          NOT NULL DEFAULT 0,
  updated bigint           NOT NULL
);
ALTER TABLE cursor REPLICA IDENTITY FULL;
