/**
 * useSeriesList — hydrate a list of SeriesSpecs into per-spec data
 * states. Used by ChartCard to drive the chart from a dynamic series
 * list (initial seed + entries added via the picker).
 *
 * Caching strategy: a single Map<specKey, HydratedSpecResult>
 * accumulates fetched results for the lifetime of the hook. New
 * specs trigger a fetch the first time they appear; specs that have
 * been fetched once stay cached even after they're removed from the
 * input list, so re-adding the same spec is instant. The cache is
 * scoped to the hook instance (per ChartCard) — across cards the data
 * is fetched independently. That's intentional and simple; if it
 * proves wasteful we can add a module-level Map later.
 */

import { useEffect, useRef, useState } from 'react'

import {
  getRegistryEntry,
  specKey,
  type SeriesPoint,
  type SeriesSpec,
} from '../data/seriesRegistry'

export interface HydratedSpecResult {
  spec: SeriesSpec
  data: SeriesPoint[]
  loading: boolean
  error: string | null
}

const PENDING: Omit<HydratedSpecResult, 'spec'> = {
  data: [],
  loading: true,
  error: null,
}

export function useSeriesList(specs: SeriesSpec[]): HydratedSpecResult[] {
  const [cache, setCache] = useState<Map<string, HydratedSpecResult>>(
    () => new Map(),
  )

  // Holds the most recent specs array so the effect can iterate it
  // without itself being a dep — the dep is the joined-keys string,
  // which changes only when the user actually adds/removes a spec
  // (not on every parent re-render).
  const specsRef = useRef(specs)
  specsRef.current = specs

  // Tracks which spec keys have already had a fetch kicked off, to
  // dedup repeat-fetches across re-renders. Refs survive across
  // strict-mode double-mounts; intentional that the ref starts empty
  // (so a remount can re-fetch — fine for our one-shot fetches).
  const startedRef = useRef<Set<string>>(new Set())

  const keysJoined = specs.map(specKey).join('|')

  useEffect(() => {
    for (const spec of specsRef.current) {
      const key = specKey(spec)
      if (startedRef.current.has(key)) continue
      const entry = getRegistryEntry(spec.registryId)
      if (!entry) {
        setCache((prev) => {
          const next = new Map(prev)
          next.set(key, {
            spec,
            data: [],
            loading: false,
            error: `Unknown series: ${spec.registryId}`,
          })
          return next
        })
        startedRef.current.add(key)
        continue
      }
      startedRef.current.add(key)
      setCache((prev) => {
        // Don't overwrite a result that already loaded (race: if the
        // user toggles a spec off and back on while a previous fetch
        // resolved between, the cache already has the result — don't
        // reset it back to loading).
        if (prev.has(key)) return prev
        const next = new Map(prev)
        next.set(key, { spec, ...PENDING })
        return next
      })
      entry.fetch(spec.district).then(
        (data) => {
          setCache((prev) => {
            const next = new Map(prev)
            next.set(key, { spec, data, loading: false, error: null })
            return next
          })
        },
        (err: unknown) => {
          const msg =
            err instanceof Error ? err.message : String(err ?? 'Fetch failed')
          setCache((prev) => {
            const next = new Map(prev)
            next.set(key, { spec, data: [], loading: false, error: msg })
            return next
          })
        },
      )
    }
    // We deliberately depend on keysJoined (not `specs`) so the effect
    // only fires when the actual set of spec keys changes — re-renders
    // that produce identity-different but value-equal arrays don't
    // trigger spurious re-fetches.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [keysJoined])

  return specs.map(
    (spec) => cache.get(specKey(spec)) ?? { spec, ...PENDING },
  )
}
