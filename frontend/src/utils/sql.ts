/**
 * Best-effort parse of a query's FROM clause into an alias -> table map.
 *
 * Handles the comma-join form the benchmarks use, with or without AS:
 *   FROM comments as c, posts as p, users u WHERE ...
 * Returns e.g. { c: 'comments', p: 'posts', u: 'users' }. Unparseable
 * entries (subqueries, etc.) are skipped — callers fall back to the alias.
 */
export function parseAliasMap(sql: string | null | undefined): Record<string, string> {
  const map: Record<string, string> = {}
  if (!sql) return map

  // Grab the FROM clause up to the next major keyword.
  const m = sql.match(/\bFROM\b([\s\S]*?)(?:\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|;|$)/i)
  if (!m) return map

  for (const rawSpec of m[1].split(',')) {
    const spec = rawSpec.trim()
    // table [AS] alias  — both identifiers simple (no subqueries/parens)
    const e = spec.match(/^([A-Za-z_][\w]*)\s+(?:as\s+)?([A-Za-z_][\w]*)$/i)
    if (e) {
      const table = e[1]
      const alias = e[2]
      if (alias.toLowerCase() === 'as') continue
      map[alias] = table
    }
  }
  return map
}
