-- Seeded BEFORE the replicator boots: initial-sync memory pressure.
-- acc_small: half of the ~200 MB compressible dataset (10,000 × ~9.8 KB).
-- Highly compressible payload (repeat) — stresses "logical bytes ≫ wire/disk".
CREATE TABLE IF NOT EXISTS acc_small (id text PRIMARY KEY, body text);
ALTER TABLE acc_small REPLICA IDENTITY FULL;
CREATE TABLE IF NOT EXISTS acc_big (id text PRIMARY KEY, body text);
ALTER TABLE acc_big REPLICA IDENTITY FULL;

INSERT INTO acc_small
SELECT 'k' || lpad(g::text, 6, '0'), repeat('orbit-payload-', 700)
FROM generate_series(1, 10000) g
ON CONFLICT (id) DO NOTHING;

-- Two of the 10 MB rows land pre-boot (initial sync must stream them).
INSERT INTO acc_big
SELECT 'big' || g, repeat(md5(g::text), 320000) -- 32 B × 320,000 = 10.24 MB
FROM generate_series(1, 2) g
ON CONFLICT (id) DO NOTHING;
