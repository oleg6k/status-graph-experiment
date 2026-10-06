"""
Synthetic experiment: status whitelists vs status graph under late, duplicated,
concurrent event delivery. Three write strategies against a real PostgreSQL.

A  adjacency-only guard in app code (one shared rule) (read, check, write); amount always
   written; a callback is emitted for every incoming event that passed the check.
B  database guards the status (CASE ... reachable), but the amount is written
   unconditionally and callbacks are emitted from incoming events.
C  status graph: transition accepted only if target is reachable from current,
   enforced in WHERE; dependent fields move with the status; callbacks come only
   from accepted transitions via an outbox written in the same statement.
"""
import random, threading, time, json, sys
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

GRAPH = {
    'queued': ['pending'],
    'pending': ['screening'],
    'screening': ['completed', 'failed', 'canceled', 'rejected'],
    'rejected': ['refund_pending'],
    'refund_pending': ['refunded', 'refund_failed'],
    'refund_failed': ['refund_pending'],
    'completed': [], 'failed': [], 'canceled': [], 'refunded': [],
}

def reach(s):
    seen, st = set(), list(GRAPH[s])
    while st:
        x = st.pop()
        if x in seen: continue
        seen.add(x); st += GRAPH[x]
    return seen
REACH = {s: reach(s) for s in GRAPH}
ALLOWED_FROM = {t: [s for s in GRAPH if t in REACH[s] and s != t] for t in GRAPH}

# --- synthetic flows (amounts in minor units; intermediate events carry provisional amounts)
def flows(n, rng):
    out = []
    for i in range(n):
        kind = rng.choices(['deposit_ok', 'deposit_refund', 'payout_ok', 'payout_fail'], [5, 2, 2, 1])[0]
        final = rng.randint(1000, 100000)
        prov = final - rng.randint(1, 900)  # e.g. amount before fee / partial confirmation
        if kind == 'deposit_ok':
            ev = [('pending', prov), ('screening', prov), ('completed', final)]; exp = ('completed', final); init = 'pending'
        elif kind == 'deposit_refund':
            ev = [('pending', prov), ('screening', prov), ('rejected', final), ('refund_pending', final),
                  ('refund_failed', final), ('refund_pending', final), ('refunded', final)]
            exp = ('refunded', final); init = 'pending'
        elif kind == 'payout_ok':
            ev = [('queued', prov), ('pending', prov), ('screening', prov), ('completed', final)]; exp = ('completed', final); init = 'queued'
        else:
            ev = [('queued', prov), ('pending', prov), ('screening', prov), ('failed', final)]; exp = ('failed', final); init = 'queued'
        out.append(dict(id=i, kind=kind, events=ev, expected=exp, init=init))
    return out

def deliver(flow, rng, dup_p=0.3):
    ev = []
    for e in flow['events'][1:]:           # first event creates the row
        ev.append(e)
        if rng.random() < dup_p: ev.append(e)
    rng.shuffle(ev)
    return ev

SCHEMA = """
DROP TABLE IF EXISTS payment, callback;
CREATE TABLE payment(id int PRIMARY KEY, status text NOT NULL, amount bigint NOT NULL);
CREATE TABLE callback(seq bigserial PRIMARY KEY, payment_id int, status text, amount bigint);
"""

def apply_A(conn, pid, status, amount):
    with conn.cursor() as c:
        c.execute("SELECT status FROM payment WHERE id=%s", (pid,))
        cur = c.fetchone()[0]
        time.sleep(random.random() * 0.002)   # handler work between read and write
        if status in GRAPH[cur]:              # adjacency whitelist, as written inline
            c.execute("UPDATE payment SET status=%s, amount=%s WHERE id=%s", (status, amount, pid))
            c.execute("INSERT INTO callback(payment_id,status,amount) VALUES(%s,%s,%s)", (pid, status, amount))
    conn.commit()

def apply_B(conn, pid, status, amount):
    with conn.cursor() as c:
        c.execute("""UPDATE payment SET
                       status = CASE WHEN status = ANY(%s) THEN %s ELSE status END,
                       amount = %s
                     WHERE id=%s""", (ALLOWED_FROM[status], status, amount, pid))
        # listener consumes the incoming event independently of the DB decision
        c.execute("INSERT INTO callback(payment_id,status,amount) VALUES(%s,%s,%s)", (pid, status, amount))
    conn.commit()

def apply_C(conn, pid, status, amount):
    with conn.cursor() as c:
        c.execute("""WITH moved AS (
                       UPDATE payment SET status=%s, amount=%s
                       WHERE id=%s AND status = ANY(%s)
                       RETURNING id, status, amount)
                     INSERT INTO callback(payment_id,status,amount)
                     SELECT id,status,amount FROM moved""", (status, amount, pid, ALLOWED_FROM[status]))
    conn.commit()

STRATS = {'A_whitelists': apply_A, 'B_db_guard_only': apply_B, 'C_status_graph': apply_C}

def run(strategy, fl, seed, workers=16):
    rng = random.Random(seed)
    pool = ThreadedConnectionPool(workers, workers + 2, host='/tmp', user='postgres')
    conn = pool.getconn()
    with conn.cursor() as c:
        c.execute(SCHEMA)
        for f in fl:
            c.execute("INSERT INTO payment VALUES(%s,%s,%s)", (f['id'], f['init'], f['events'][0][1]))
            c.execute("INSERT INTO callback(payment_id,status,amount) VALUES(%s,%s,%s)", (f['id'], f['init'], f['events'][0][1]))
    conn.commit(); pool.putconn(conn)
    jobs = []
    for f in fl:
        for st, am in deliver(f, rng):
            jobs.append((f['id'], st, am))
    rng.shuffle(jobs)  # interleave flows too
    fn = STRATS[strategy]
    def work(j):
        cn = pool.getconn()
        try:
            time.sleep(random.random() * 0.003)
            fn(cn, *j)
        finally:
            pool.putconn(cn)
    t0 = time.time()
    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(work, jobs))
    dt = time.time() - t0
    conn = pool.getconn()
    with conn.cursor() as c:
        c.execute("SELECT id,status,amount FROM payment"); rows = {r[0]: r[1:] for r in c.fetchall()}
        c.execute("SELECT payment_id,status,amount FROM callback ORDER BY seq"); cbs = c.fetchall()
    pool.putconn(conn); pool.closeall()
    stream = {}
    for pid, st, am in cbs: stream.setdefault(pid, []).append((st, am))
    m = dict(flows=len(fl), wrong_status=0, amount_mismatch=0, backward_callback=0,
             dup_callbacks=0, extra_retry_flips=0, callbacks=0, ideal_callbacks=0, events=len(jobs), seconds=round(dt, 2))
    for f in fl:
        st, am = rows[f['id']]; est, eam = f['expected']
        if st != est: m['wrong_status'] += 1
        elif am != eam: m['amount_mismatch'] += 1
        s = stream.get(f['id'], [])
        m['callbacks'] += len(s)
        m['ideal_callbacks'] += len(f['events'])
        statuses = [x[0] for x in s]
        back = any(b not in REACH[a] for a, b in zip(statuses, statuses[1:]))
        if back: m['backward_callback'] += 1
        if any(a == b for a, b in zip(statuses, statuses[1:])): m['dup_callbacks'] += 1
        flips = sum(1 for a, b in zip(statuses, statuses[1:]) if (a, b) == ('refund_pending', 'refund_failed'))
        if flips > 1: m['extra_retry_flips'] += 1   # ideal path contains exactly one retry
    return m

if __name__ == '__main__':
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    res = {}
    for strat in STRATS:
        agg = None
        for r in range(reps):
            fl = flows(N, random.Random(100 + r))
            m = run(strat, fl, seed=200 + r)
            agg = m if agg is None else {k: agg[k] + m[k] for k in m}
        res[strat] = agg
        print(strat, json.dumps(agg), flush=True)
    json.dump(res, open('results.json', 'w'), indent=1)
