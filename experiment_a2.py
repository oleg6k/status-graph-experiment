"""Run only strategy A2 (kept for backward compatibility; prefer run_all.py)."""
import json, random
import experiment as exp
import strategies_extra  # noqa: F401  registers A2, A3

if __name__ == '__main__':
    runs = [exp.run('A2_reachability_in_app_non_atomic', exp.flows(1000, random.Random(100 + r)), seed=200 + r) for r in range(5)]
    print(json.dumps(runs, indent=1))
