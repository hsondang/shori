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

export function formatExecutionTime(ms: number): string {
  const totalMs = Math.max(0, Math.round(ms))
  const hours = Math.floor(totalMs / 3_600_000)
  const minutes = Math.floor((totalMs % 3_600_000) / 60_000)
  const seconds = Math.floor((totalMs % 60_000) / 1_000)
  const millis = totalMs % 1_000

  const parts: string[] = []
  if (hours > 0) parts.push(`${hours}h`)
  if (minutes > 0) parts.push(`${minutes}m`)
  if (seconds > 0) parts.push(`${seconds}s`)
  if (millis > 0) parts.push(`${millis}ms`)
  return parts.length > 0 ? parts.join(' ') : '0ms'
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
