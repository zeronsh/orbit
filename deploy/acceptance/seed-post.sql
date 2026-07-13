-- Seeded AFTER the cluster is ready: replication + change-stream + durable
-- change-log pressure (every row transits the ring, the log queue, and the
-- broadcast). Batched so single transactions stay reasonable.
INSERT INTO acc_small
SELECT 'k' || lpad(g::text, 6, '0'), repeat('orbit-payload-', 700)
FROM generate_series(10001, 12500) g
ON CONFLICT (id) DO NOTHING;

INSERT INTO acc_small
SELECT 'k' || lpad(g::text, 6, '0'), repeat('orbit-payload-', 700)
FROM generate_series(12501, 15000) g
ON CONFLICT (id) DO NOTHING;

INSERT INTO acc_small
SELECT 'k' || lpad(g::text, 6, '0'), repeat('orbit-payload-', 700)
FROM generate_series(15001, 17500) g
ON CONFLICT (id) DO NOTHING;

INSERT INTO acc_small
SELECT 'k' || lpad(g::text, 6, '0'), repeat('orbit-payload-', 700)
FROM generate_series(17501, 20000) g
ON CONFLICT (id) DO NOTHING;

-- Three more 10 MB rows arrive LIVE (individual huge events through the
-- byte-bounded ring and bounded log queue).
INSERT INTO acc_big
SELECT 'big' || g, repeat(md5(g::text), 320000)
FROM generate_series(3, 5) g
ON CONFLICT (id) DO NOTHING;
