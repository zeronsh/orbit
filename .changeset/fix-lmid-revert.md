---
"@zeronsh/orbit": patch
---

Fix optimistic rows reverting while another client writes.

The view-syncer used to flush a client's `lastMutationID` ack on any replication
tick — so another client's write (its own tick) could confirm your mutation
before your row returned via replication, dropping your optimistic overlay for a
beat (the "my pixels revert while someone else draws" bug). The ack now rides
atomically with the mutation's own rows: it's derived from the replicated
`orbit_client_mutations` table (written by the PushProcessor in the same
transaction as the data), so a client's ack and its rows always land in the same
commit → same poke. `orbit_client_mutations` is now included in the replication
publication automatically. Server/binary only — no TS API change.
