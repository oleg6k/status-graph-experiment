# Status graph experiment

Companion code for the article *"Status Graphs, Not Status Whitelists: Accepting Late Events Without Moving Backwards"*.

A synthetic reproduction of late, duplicated and out-of-order status events hitting a payment record in PostgreSQL, comparing five write strategies:

| Strategy | Description |
|---|---|
| A | Adjacency-only guard in application code, one shared rule (read, check, write) |
| A3 | Drifted per-handler whitelists in application code (each handler has its own list; one negative check) |
| A2 | Transitive reachability rule in application code, non-atomic |
| B | Database guards the status (`CASE … reachable`); amount and callbacks follow incoming events |
| C | Status graph: reachability enforced in `WHERE`, status-dependent fields move with the status, callbacks only from accepted transitions (outbox in the same statement) |

## Setup

- 1,000 flows per run (deposit, deposit with refund retry loop, successful payout, failed payout), 5 runs per strategy
- 30% event duplication, random order, 16 concurrent workers
- Intermediate events carry a provisional amount, terminal events the final one

## Results (PostgreSQL 16, range across 5 runs, per 1,000 flows)

| Strategy | Wrong final status | Final status, wrong amount | Callback stream moves backward | Duplicate callbacks | Extra retry-loop passes | Callbacks sent (ideal 4,052–4,188) |
|---|---|---|---|---|---|---|
| A | 606–636 | 0 | 2–4 | 2–4 | 0–3 | 2,780–2,816 |
| A3 | 168–206 | 0 | 93–108 | 1–7 | 5–16 | 3,029–3,137 |
| A2 | 0–7 | 0 | 3–15 | 63–85 | 2–6 | 2,923–2,964 |
| B | 0 | 480–493 | 840–869 | 378–399 | 1–8 | 4,949–5,136 |
| C | **0** | **0** | **0** | **0** | 2–6 | 2,824–2,849 |

Per-run raw numbers: `results.json`. Worker timing uses an unseeded RNG, so exact counts vary between runs; the pattern does not.

## Run

```bash
# PostgreSQL reachable via unix socket in /tmp as user postgres (trust auth)
pip install psycopg2-binary
python run_all.py 5 1000     # runs, flows per run
```

## License

MIT
