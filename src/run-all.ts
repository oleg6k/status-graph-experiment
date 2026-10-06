/** Run every strategy RUNS times (N flows each); print min–max per metric and write results.json. */
import { writeFileSync } from 'node:fs'
import { flows, rng, run, STRATEGIES, Metrics } from './experiment'

const RUNS = Number(process.argv[2] ?? 5)
const N = Number(process.argv[3] ?? 1000)

async function main() {
  const out: Record<string, Metrics[]> = {}
  for (const s of Object.keys(STRATEGIES)) {
    const runs: Metrics[] = []
    for (let i = 0; i < RUNS; i++) runs.push(await run(s, flows(N, rng(100 + i)), 200 + i))
    out[s] = runs
    const keys = ['wrong_status', 'amount_mismatch', 'backward_callback', 'dup_callbacks',
      'extra_retry_flips', 'callbacks', 'ideal_callbacks'] as const
    const range = Object.fromEntries(keys.map(k => {
      const v = runs.map(r => r[k])
      return [k, `${Math.min(...v)}–${Math.max(...v)}`]
    }))
    console.log(s, JSON.stringify(range))
  }
  writeFileSync('results.json', JSON.stringify(out, null, 1))
}

main().catch(e => { console.error(e); process.exit(1) })
