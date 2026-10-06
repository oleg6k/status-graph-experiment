"""Run every strategy RUNS times (1,000 flows each) and report per-run results and min-max."""
import json, random, sys
import experiment as exp
import strategies_extra  # registers A2, A3

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
N = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
order = ['A_whitelists', 'A3_drifted_handler_whitelists', 'A2_reachability_in_app_non_atomic', 'B_db_guard_only', 'C_status_graph']
out = {}
for s in order:
    runs = [exp.run(s, exp.flows(N, random.Random(100 + r)), seed=200 + r) for r in range(RUNS)]
    out[s] = runs
    keys = ['wrong_status', 'amount_mismatch', 'backward_callback', 'dup_callbacks', 'extra_retry_flips', 'callbacks', 'ideal_callbacks']
    print(s, {k: (min(r[k] for r in runs), max(r[k] for r in runs)) for k in keys}, flush=True)
json.dump(out, open('results.json', 'w'), indent=1)
