import type { ExecutionRunStatus, NodeExecutionResult } from '../types/pipeline'

export function parseExecutionTimestamp(value?: string): number | null {
  if (!value) return null
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? null : parsed
}

export function formatElapsedDuration(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000))
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  if (hours > 0) {
    return `${hours}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`
  }

  return `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`
}

export function getResultElapsedLabel(result: NodeExecutionResult, nowMs: number): string | null {
  if (result.status !== 'running') return null
  const startedAtMs = parseExecutionTimestamp(result.started_at)
  if (startedAtMs == null) return null
  return formatElapsedDuration(nowMs - startedAtMs)
}

export function getRunElapsedLabel(run: ExecutionRunStatus | null | undefined, nowMs: number): string | null {
  if (!run || run.status !== 'running') return null
  const startedAtMs = parseExecutionTimestamp(run.started_at)
  if (startedAtMs == null) return null
  return formatElapsedDuration(nowMs - startedAtMs)
}
