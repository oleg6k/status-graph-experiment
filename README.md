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

| Strategy | Wrong final status | Final status, wrong amount | Callback stream moves backward | Duplicate callbacks | Extra retry-loop passes | Callbacks sent (ideal 4,053–4,188) |
|---|---|---|---|---|---|---|
| A | 591–618 | 0 | 0–3 | 0–3 | 0–1 | 2,753–2,837 |
| A3 | 157–190 | 0 | 95–106 | 1–7 | 5–13 | 3,072–3,118 |
| A2 | 2–6 | 0 | 7–12 | 65–85 | 1–2 | 2,909–2,970 |
| B | 0 | 457–484 | 846–863 | 381–421 | 1–6 | 4,973–5,120 |
| C | **0** | **0** | **0** | **0** | 1–2 | 2,826–2,855 |

Per-run raw numbers: `results.json`. Flows and delivery order are seeded; worker timing is not, so exact counts vary between runs. The pattern does not.

## Run

Requires Node.js 20+ and a PostgreSQL 16 instance (defaults: unix socket in `/tmp`, user `postgres`; override with `PGHOST`, `PGUSER`, etc.).

```bash
npm install
npm start              # 5 runs × 1,000 flows per strategy
npm run typecheck
```

Code: `src/experiment.ts` (graph, flows, strategies, metrics), `src/run-all.ts` (runner).

## License

MIT
