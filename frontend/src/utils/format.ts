/**
 * Shared number/duration formatting helpers.
 *
 * formatNumber mirrors the canonical implementation that was previously copied
 * privately into several components (PeakIntermediateBar, the DAG/flow
 * visualizations, …) so row counts read identically everywhere — "917.00M",
 * "1.0k", "224".
 */
export function formatNumber(n: number | undefined | null): string {
  const v = Number(n) || 0
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}B`
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k`
  return Math.round(v).toLocaleString()
}

/** Milliseconds → human duration: "882ms", "1.5s", "90s". */
export function formatDuration(ms: number | undefined | null): string {
  const v = Number(ms) || 0
  if (v >= 1000) return `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}s`
  return `${Math.round(v)}ms`
}
