# Schema v2
Adds `lock_waits` / `lock_wait_links` via SQLAlchemy metadata `create_all` (imported by `scripts/seed_demo.py`).

`lock_waits` records one continuous blocked result-write episode per trace: `phase` (`result-write`), the worker's `blocked_pid`, the wait window (`started_at`/`resolved_at`/`duration_ms`), and collection retry state (`collect_attempts`/`collect_error`). A retry after a failed collection reuses the open wait row — the same continuous wait is never recorded twice. `trace_id` is deliberately not a foreign key: an FK insert check would take a KEY SHARE lock on the referenced `traces` row and drag the watchdog into the very lock wait it is observing.

`lock_wait_links` stores the chain traced from this write (position 0) to the last lock-holding transaction. `evidence` (backend xid / xact_start) is server-side only, used to re-verify links during the same wait: matching evidence marks `confirmed`; concurrent unlock or connection reuse breaks the evidence and marks `unconfirmed`. After the wait resolves, confirmed chains stay with the profile; holder links expose `backend_type` only — never statements or sampled values.
