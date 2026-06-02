/**
 * Loads the reference/optimized Spark SQL configs the user set in Settings
 * (persisted under localStorage 'sparkSqlConfig'), falling back to the demo
 * defaults — reference = optimisation OFF, optimized = Yannakakis + physical
 * CountJoin + unguarded ON. This is the same contract ExecuteView builds inline
 * before calling /execution/execute; factored out so the Batch runner uses an
 * identical configuration without duplicating the parsing.
 */
import type { SparkConfig } from '@/types'

const REFERENCE_DEFAULT: SparkConfig = {
  yannakakis_enabled: false,
  physical_count_join_enabled: false,
  unguarded_enabled: false,
  custom_options: null,
}

const OPTIMIZED_DEFAULT: SparkConfig = {
  yannakakis_enabled: true,
  physical_count_join_enabled: true,
  unguarded_enabled: true,
  custom_options: null,
}

function toCustomOptions(opts: any): Record<string, string> | null {
  if (!Array.isArray(opts)) return null
  const out: Record<string, string> = {}
  opts
    .filter((o: any) => o && o.key && o.value)
    .forEach((o: any) => {
      out[o.key] = o.value
    })
  return Object.keys(out).length > 0 ? out : null
}

export function loadSparkConfigs(): { referenceConfig: SparkConfig; optimizedConfig: SparkConfig } {
  const saved = localStorage.getItem('sparkSqlConfig')
  if (!saved) {
    return { referenceConfig: { ...REFERENCE_DEFAULT }, optimizedConfig: { ...OPTIMIZED_DEFAULT } }
  }
  try {
    const parsed = JSON.parse(saved)
    return {
      referenceConfig: {
        yannakakis_enabled: parsed.referenceConfig?.yannakakis_enabled ?? false,
        physical_count_join_enabled: parsed.referenceConfig?.physical_count_join_enabled ?? false,
        unguarded_enabled: parsed.referenceConfig?.unguarded_enabled ?? false,
        custom_options: toCustomOptions(parsed.referenceCustomOptions),
      },
      optimizedConfig: {
        yannakakis_enabled: parsed.optimizedConfig?.yannakakis_enabled ?? true,
        physical_count_join_enabled: parsed.optimizedConfig?.physical_count_join_enabled ?? true,
        unguarded_enabled: parsed.optimizedConfig?.unguarded_enabled ?? true,
        custom_options: toCustomOptions(parsed.optimizedCustomOptions),
      },
    }
  } catch {
    return { referenceConfig: { ...REFERENCE_DEFAULT }, optimizedConfig: { ...OPTIMIZED_DEFAULT } }
  }
}
