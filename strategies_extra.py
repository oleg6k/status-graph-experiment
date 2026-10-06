"""Additional strategies: A2 (reachability in app code, non-atomic) and A3 (drifted per-handler whitelists)."""
import random
import experiment as exp

def apply_A2(conn, pid, status, amount):
    with conn.cursor() as c:
        c.execute("SELECT status FROM payment WHERE id=%s", (pid,))
        cur = c.fetchone()[0]
        exp.time.sleep(random.random() * 0.002)
        if status in exp.REACH[cur]:
            c.execute("UPDATE payment SET status=%s, amount=%s WHERE id=%s", (status, amount, pid))
            c.execute("INSERT INTO callback(payment_id,status,amount) VALUES(%s,%s,%s)", (pid, status, amount))
    conn.commit()

# Per-handler whitelists as they tend to look after a few years: each handler has its own
# list of "allowed current statuses", edited independently. Some are too strict, some too lenient,
# one uses a negative check that lets a terminal record move.
DRIFTED = {
    'pending':        lambda cur: cur == 'queued',
    'screening':      lambda cur: cur in ('pending', 'queued'),
    'completed':      lambda cur: cur in ('screening', 'pending'),
    'failed':         lambda cur: cur in ('screening', 'pending', 'queued'),
    'canceled':       lambda cur: cur in ('screening', 'pending'),
    'rejected':       lambda cur: cur == 'screening',
    'refund_pending': lambda cur: cur not in ('completed', 'refund_pending'),   # negative check
    'refund_failed':  lambda cur: cur == 'refund_pending',
    'refunded':       lambda cur: cur in ('refund_pending', 'refund_failed'),
}

def apply_A3(conn, pid, status, amount):
    with conn.cursor() as c:
        c.execute("SELECT status FROM payment WHERE id=%s", (pid,))
        cur = c.fetchone()[0]
        exp.time.sleep(random.random() * 0.002)
        if DRIFTED[status](cur):
            c.execute("UPDATE payment SET status=%s, amount=%s WHERE id=%s", (status, amount, pid))
            c.execute("INSERT INTO callback(payment_id,status,amount) VALUES(%s,%s,%s)", (pid, status, amount))
    conn.commit()

exp.STRATS['A2_reachability_in_app_non_atomic'] = apply_A2
exp.STRATS['A3_drifted_handler_whitelists'] = apply_A3
