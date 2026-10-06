/**
 * Synthetic experiment: status whitelists vs status graph under late, duplicated,
 * out-of-order and concurrent event delivery, against a real PostgreSQL.
 *
 * A   adjacency-only guard in application code (one shared rule): read, check, write
 * A3  drifted per-handler whitelists in application code (each handler has its own list)
 * A2  transitive reachability rule in application code, non-atomic
 * B   database guards the status; amount and callbacks follow incoming events
 * C   status graph: reachability enforced in WHERE, status-dependent fields move with
 *     the status, callbacks only from accepted transitions (outbox in the same statement)
 */
import { Pool, PoolClient } from 'pg'

export type Status =
  | 'queued' | 'pending' | 'screening'
  | 'completed' | 'failed' | 'canceled'
  | 'rejected' | 'refund_pending' | 'refunded' | 'refund_failed'

export const GRAPH: Record<Status, Status[]> = {
  queued: ['pending'],
  pending: ['screening'],
  screening: ['completed', 'failed', 'canceled', 'rejected'],
  rejected: ['refund_pending'],
  refund_pending: ['refunded', 'refund_failed'],
  refund_failed: ['refund_pending'], // retry loop
  completed: [], failed: [], canceled: [], refunded: [],
}

const STATUSES = Object.keys(GRAPH) as Status[]

function reachableFrom(from: Status): Set<Status> {
  const seen = new Set<Status>()
  const stack = [...GRAPH[from]]
  while (stack.length) {
    const s = stack.pop()!
    if (seen.has(s)) continue
    seen.add(s)
    stack.push(...GRAPH[s])
  }
  return seen
}

export const REACH = Object.fromEntries(STATUSES.map(s => [s, reachableFrom(s)])) as Record<Status, Set<Status>>
/** For every target status: the statuses from which it is reachable (the `allowed_current` set). */
export const ALLOWED_FROM = Object.fromEntries(
  STATUSES.map(t => [t, STATUSES.filter(s => s !== t && REACH[s].has(t))]),
) as Record<Status, Status[]>

// ---------- seeded RNG (flows and delivery order are reproducible; worker timing is not)
export function rng(seed: number) {
  let a = seed >>> 0
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  return {
    next,
    int: (lo: number, hi: number) => lo + Math.floor(next() * (hi - lo + 1)),
    shuffle<T>(xs: T[]) {
      for (let i = xs.length - 1; i > 0; i--) {
        const j = Math.floor(next() * (i + 1));
        [xs[i], xs[j]] = [xs[j], xs[i]]
      }
      return xs
    },
    pick<T>(xs: T[], weights: number[]) {
      const total = weights.reduce((a, b) => a + b, 0)
      let r = next() * total
      for (let i = 0; i < xs.length; i++) { r -= weights[i]; if (r < 0) return xs[i] }
      return xs[xs.length - 1]
    },
  }
}
type Rng = ReturnType<typeof rng>

// ---------- synthetic flows: intermediate events carry a provisional amount, terminal ones the final
export type Event = { status: Status; amount: number }
export type Flow = { id: number; kind: string; events: Event[]; expected: Event; init: Status }

export function flows(n: number, r: Rng): Flow[] {
  const out: Flow[] = []
  for (let id = 0; id < n; id++) {
    const kind = r.pick(['deposit_ok', 'deposit_refund', 'payout_ok', 'payout_fail'], [5, 2, 2, 1])
    const final = r.int(1000, 100000)
    const prov = final - r.int(1, 900) // e.g. amount before fee / partial confirmation
    const e = (status: Status, amount: number): Event => ({ status, amount })
    let events: Event[]
    let expected: Event
    if (kind === 'deposit_ok') {
      events = [e('pending', prov), e('screening', prov), e('completed', final)]; expected = e('completed', final)
    } else if (kind === 'deposit_refund') {
      events = [e('pending', prov), e('screening', prov), e('rejected', final), e('refund_pending', final),
        e('refund_failed', final), e('refund_pending', final), e('refunded', final)]
      expected = e('refunded', final)
    } else if (kind === 'payout_ok') {
      events = [e('queued', prov), e('pending', prov), e('screening', prov), e('completed', final)]; expected = e('completed', final)
    } else {
      events = [e('queued', prov), e('pending', prov), e('screening', prov), e('failed', final)]; expected = e('failed', final)
    }
    out.push({ id, kind, events, expected, init: events[0].status })
  }
  return out
}

function deliver(f: Flow, r: Rng, dupP = 0.3): Event[] {
  const ev: Event[] = []
  for (const e of f.events.slice(1)) { // the first event creates the row
    ev.push(e)
    if (r.next() < dupP) ev.push(e)
  }
  return r.shuffle(ev)
}

const SCHEMA = `
DROP TABLE IF EXISTS payment, callback;
CREATE TABLE payment(id int PRIMARY KEY, status text NOT NULL, amount bigint NOT NULL);
CREATE TABLE callback(seq bigserial PRIMARY KEY, payment_id int, status text, amount bigint);`

const sleep = (ms: number) => new Promise(res => setTimeout(res, ms))

// ---------- write strategies
type Apply = (c: PoolClient, id: number, e: Event) => Promise<void>

/** Read, check, write in application code with a given rule; amount always written; callback per passed event. */
const readCheckWrite = (allowed: (current: Status, target: Status) => boolean): Apply => async (c, id, e) => {
  await c.query('BEGIN')
  const { rows } = await c.query('SELECT status FROM payment WHERE id=$1', [id])
  await sleep(Math.random() * 2) // handler work between read and write
  if (allowed(rows[0].status, e.status)) {
    await c.query('UPDATE payment SET status=$1, amount=$2 WHERE id=$3', [e.status, e.amount, id])
    await c.query('INSERT INTO callback(payment_id,status,amount) VALUES($1,$2,$3)', [id, e.status, e.amount])
  }
  await c.query('COMMIT')
}

/** Per-handler whitelists as they tend to look after a few years of independent edits. */
const DRIFTED: Record<Status, (current: Status) => boolean> = {
  queued: () => false,
  pending: cur => cur === 'queued',
  screening: cur => ['pending', 'queued'].includes(cur),
  completed: cur => ['screening', 'pending'].includes(cur),
  failed: cur => ['screening', 'pending', 'queued'].includes(cur),
  canceled: cur => ['screening', 'pending'].includes(cur),
  rejected: cur => cur === 'screening',
  refund_pending: cur => !['completed', 'refund_pending'].includes(cur), // negative check
  refund_failed: cur => cur === 'refund_pending',
  refunded: cur => ['refund_pending', 'refund_failed'].includes(cur),
}

export const STRATEGIES: Record<string, Apply> = {
  A_adjacency_only: readCheckWrite((cur, t) => GRAPH[cur].includes(t)),
  A3_drifted_handler_whitelists: readCheckWrite((cur, t) => DRIFTED[t](cur)),
  A2_reachability_in_app_non_atomic: readCheckWrite((cur, t) => REACH[cur].has(t)),

  B_db_guard_only: async (c, id, e) => {
    await c.query('BEGIN')
    await c.query(
      `UPDATE payment SET
         status = CASE WHEN status = ANY($1::text[]) THEN $2 ELSE status END,
         amount = $3
       WHERE id = $4`,
      [ALLOWED_FROM[e.status], e.status, e.amount, id])
    // the notifier consumes the incoming event, independently of the database decision
    await c.query('INSERT INTO callback(payment_id,status,amount) VALUES($1,$2,$3)', [id, e.status, e.amount])
    await c.query('COMMIT')
  },

  C_status_graph: async (c, id, e) => {
    await c.query(
      `WITH moved AS (
         UPDATE payment SET status = $1, amount = $2
         WHERE id = $3 AND status = ANY($4::text[])
         RETURNING id, status, amount)
       INSERT INTO callback(payment_id, status, amount)
       SELECT id, status, amount FROM moved`,
      [e.status, e.amount, id, ALLOWED_FROM[e.status]])
  },
}

export type Metrics = {
  flows: number; events: number
  wrong_status: number; amount_mismatch: number; backward_callback: number
  dup_callbacks: number; extra_retry_flips: number; callbacks: number; ideal_callbacks: number
}

export async function run(strategy: string, fl: Flow[], seed: number, workers = 16): Promise<Metrics> {
  const r = rng(seed)
  const pool = new Pool({ host: process.env.PGHOST ?? '/tmp', user: process.env.PGUSER ?? 'postgres', max: workers + 2 })
  await pool.query(SCHEMA)
  for (const f of fl) {
    await pool.query('INSERT INTO payment VALUES($1,$2,$3)', [f.id, f.init, f.events[0].amount])
    await pool.query('INSERT INTO callback(payment_id,status,amount) VALUES($1,$2,$3)', [f.id, f.init, f.events[0].amount])
  }
  const jobs = r.shuffle(fl.flatMap(f => deliver(f, r).map(e => ({ id: f.id, e })))) // interleave flows
  const apply = STRATEGIES[strategy]
  let next = 0
  await Promise.all(Array.from({ length: workers }, async () => {
    while (next < jobs.length) {
      const job = jobs[next++]
      const c = await pool.connect()
      try {
        await sleep(Math.random() * 3)
        await apply(c, job.id, job.e)
      } finally { c.release() }
    }
  }))
  const rows = new Map(
    (await pool.query<{ id: number; status: Status; amount: string }>('SELECT id,status,amount FROM payment')).rows
      .map(x => [x.id, x] as const))
  const stream = new Map<number, Status[]>()
  for (const cb of (await pool.query<{ payment_id: number; status: Status }>('SELECT payment_id,status FROM callback ORDER BY seq')).rows) {
    if (!stream.has(cb.payment_id)) stream.set(cb.payment_id, [])
    stream.get(cb.payment_id)!.push(cb.status)
  }
  await pool.end()

  const m: Metrics = { flows: fl.length, events: jobs.length, wrong_status: 0, amount_mismatch: 0,
    backward_callback: 0, dup_callbacks: 0, extra_retry_flips: 0, callbacks: 0, ideal_callbacks: 0 }
  for (const f of fl) {
    const row = rows.get(f.id)!
    if (row.status !== f.expected.status) m.wrong_status++
    else if (Number(row.amount) !== f.expected.amount) m.amount_mismatch++
    const s = stream.get(f.id) ?? []
    m.callbacks += s.length
    m.ideal_callbacks += f.events.length
    const pairs = s.slice(1).map((b, i) => [s[i], b] as const)
    if (pairs.some(([a, b]) => !REACH[a].has(b))) m.backward_callback++
    if (pairs.some(([a, b]) => a === b)) m.dup_callbacks++
    // the ideal path contains exactly one retry
    if (pairs.filter(([a, b]) => a === 'refund_pending' && b === 'refund_failed').length > 1) m.extra_retry_flips++
  }
  return m
}
