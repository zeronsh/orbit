---
"@zeronsh/orbit": patch
---

Fix permanent, asymmetric sync divergence across reloads (two devices showing
different state, one device's writes never reaching the other even after refresh).

The client persisted its resume **cookie** immediately on every `pokeEnd`, but the
**rows** that cookie covers only flushed to IndexedDB on a 50 ms debounce. A reload
in that window (common while another user was actively drawing) restored a cookie
that was *ahead* of the durable rows. On reconnect the client sent that cookie as
`baseCookie`; the server's delta-resume matched it exactly and suppressed the rows
the client had never actually stored — so those rows were lost forever, and only a
CVR reset could recover the device.

The cookie is now owned and persisted by the row store, written in `flush()`
**after** the row writes it covers (and loaded back in `hydrate()`). This enforces
Zero's invariant that the persisted cookie is never ahead of the persisted rows: a
crash/reload can only ever restore a cookie at or behind the durable rows, so the
server re-sends anything missing (idempotently) instead of suppressing it.
