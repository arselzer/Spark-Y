import type { QueryCategory } from './index'

export interface BenchmarkInfo {
  label: string
  fullName: string
  description: string
  tableNote: string
  badgeClass: string
}

export const BENCHMARKS: Record<QueryCategory, BenchmarkInfo> = {
  job: {
    label: 'JOB',
    fullName: 'Join Order Benchmark',
    description: 'Real-world IMDB queries (Leis et al., 2015) — heavy joins over a movie database.',
    tableNote: 'IMDB schema; load via the bundled sql-dump.',
    badgeClass: 'badge-job',
  },
  tpch: {
    label: 'TPC-H',
    fullName: 'TPC-H Decision Support',
    description: 'Industry-standard analytical workload over an order-management schema.',
    tableNote: 'Bundled: data/sql-dumps/tpch.sql.gz (SF 0.05, ~433k rows across 8 tables).',
    badgeClass: 'badge-tpch',
  },
  tpcds: {
    label: 'TPC-DS',
    fullName: 'TPC Decision Support',
    description: 'Larger and more complex than TPC-H; star/snowflake schemas.',
    tableNote: 'Generate with dsdgen.',
    badgeClass: 'badge-tpcds',
  },
  'stats-ceb': {
    label: 'STATS-CEB',
    fullName: 'Stack Exchange Cardinality Estimation Benchmark',
    description: 'Han et al. cardinality-estimation benchmark over the Stats Stack Exchange dump.',
    tableNote: 'Bundled: data/sql-dumps/stats.sql.gz (8 tables, ~1M rows).',
    badgeClass: 'badge-stats',
  },
  snap: {
    label: 'SNAP',
    fullName: 'Stanford Network Analysis Project',
    description: 'Self-joins over real-world graphs (DBLP, Google, Patents, Wiki) — path and tree patterns.',
    tableNote: 'Bundled: data/sql-dumps/snap_dblp.sql.gz (DBLP, ~1M edges). Add Google/Patents/Wiki with scripts/fetch_snap_graphs.py.',
    badgeClass: 'badge-snap',
  },
  custom: {
    label: 'Custom',
    fullName: 'Custom Query',
    description: 'User-authored SQL.',
    tableNote: '',
    badgeClass: 'badge-custom',
  },
}

export function benchmarkLabel(category: QueryCategory | string): string {
  return BENCHMARKS[category as QueryCategory]?.label ?? String(category).toUpperCase()
}

export function benchmarkInfo(category: QueryCategory | string): BenchmarkInfo {
  return BENCHMARKS[category as QueryCategory] ?? BENCHMARKS.custom
}

export interface FeaturedQuery {
  /** Backend query id, e.g. "stats_009-033" */
  queryId: string
  /** Human label, e.g. "STATS 009-033" */
  label: string
  /** Observed speedup against the bundled data dump */
  speedup: number
  /** One-line hook for why this query is worth demoing */
  blurb: string
}

// Picked from a full sweep of the bundled STATS-CEB dump
// (see commit log; ref vs optimised with yannakakis+pcj+unguarded).
// First three are the headline picks: reference exhausts the 90s budget
// while the optimised plan completes in 1-6s. The remaining ones are
// ordered by measured speedup descending.
export const FEATURED_QUERIES: FeaturedQuery[] = [
  { queryId: 'stats_002-048', label: 'STATS 002-048', speedup: 90,   blurb: 'ref times out at 90s → opt 1.0s; output = 224 rows (intermediate must be huge)' },
  { queryId: 'stats_114-049', label: 'STATS 114-049', speedup: 90,   blurb: 'ref times out at 90s → opt 0.9s; output = 18.6M rows' },
  { queryId: 'stats_116-032', label: 'STATS 116-032', speedup: 90,   blurb: 'ref times out at 90s → opt 6.3s; output = 19.4M rows' },
  { queryId: 'stats_009-033', label: 'STATS 009-033', speedup: 20.2, blurb: '8-way join, 72s → 3.6s; output = 6.3k rows' },
  { queryId: 'stats_011-050', label: 'STATS 011-050', speedup: 16.0, blurb: '5-way join, 23.5s → 1.5s; output = 9.8k rows' },
  { queryId: 'stats_065-012', label: 'STATS 065-012', speedup: 12.2, blurb: '5-way join, 3.8s → 308ms; output = 1.0M rows' },
  { queryId: 'stats_144-122', label: 'STATS 144-122', speedup: 10.2, blurb: '6-way join, 9.0s → 882ms; output = 11.2B rows' },
  { queryId: 'stats_056-007', label: 'STATS 056-007', speedup: 9.1,  blurb: '5-way join, 5.1s → 561ms; output = 699k rows' },
  { queryId: 'stats_047-008', label: 'STATS 047-008', speedup: 6.8,  blurb: '6-way join, 3.7s → 549ms; output = 481k rows' },
  { queryId: 'stats_146-058', label: 'STATS 146-058', speedup: 5.3,  blurb: '11-way join, 17.3s → 3.3s; output = 17.8B rows' },
]

export const FEATURED_QUERY_IDS = new Set(FEATURED_QUERIES.map(q => q.queryId))
