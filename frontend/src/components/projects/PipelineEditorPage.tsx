import { useCallback, useEffect, useRef, useState } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import { useParams } from 'react-router-dom'
import FlowCanvas from '../flow/FlowCanvas'
import NodeErrorDialog from '../flow/NodeErrorDialog'
import NodeConfigPanel from '../panels/NodeConfigPanel'
import DataPreviewPanel from '../panels/DataPreviewPanel'
import { usePipelineStore } from '../../store/pipelineStore'
import {
  clampPreviewHeight,
  COLLAPSED_PREVIEW_HEIGHT_PX,
  DEFAULT_PREVIEW_HEIGHT_PX,
  TOP_WORKSPACE_MIN_HEIGHT_PX,
} from './pipelineEditorLayout'

export default function PipelineEditorPage() {
  const { projectId } = useParams()
  const pipelineId = usePipelineStore((s) => s.pipelineId)
  const loadPipeline = usePipelineStore((s) => s.loadPipeline)
  const [status, setStatus] = useState<'loading' | 'ready' | 'missing'>('loading')
  const [previewCollapsed, setPreviewCollapsed] = useState(false)
  const [previewHeightPx, setPreviewHeightPx] = useState(DEFAULT_PREVIEW_HEIGHT_PX)
  const [editorHeightPx, setEditorHeightPx] = useState(0)
  const layoutRef = useRef<HTMLDivElement>(null)
  const resizeStateRef = useRef<{ startY: number; startHeightPx: number; editorHeightPx: number } | null>(null)

  useEffect(() => {
    if (!projectId) {
      setStatus('missing')
      return
    }

    if (pipelineId === projectId) {
      setStatus('ready')
      return
    }

    let cancelled = false
    setStatus('loading')

    void loadPipeline(projectId)
      .then(() => {
        if (!cancelled) {
          setStatus('ready')
        }
      })
      .catch(() => {
        if (!cancelled) {
          setStatus('missing')
        }
      })

    return () => {
      cancelled = true
    }
  }, [loadPipeline, pipelineId, projectId])

  useEffect(() => {
    const layoutElement = layoutRef.current
    if (!layoutElement) {
      return
    }

    const updateEditorHeight = () => {
      setEditorHeightPx(layoutElement.clientHeight)
    }

    updateEditorHeight()

    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', updateEditorHeight)
      return () => {
        window.removeEventListener('resize', updateEditorHeight)
      }
    }

    const observer = new ResizeObserver(() => {
      updateEditorHeight()
    })
    observer.observe(layoutElement)

    return () => {
      observer.disconnect()
    }
  }, [])

  useEffect(() => {
    if (previewCollapsed) {
      return
    }

    setPreviewHeightPx((currentHeightPx) => clampPreviewHeight(currentHeightPx, editorHeightPx))
  }, [editorHeightPx, previewCollapsed])

  const stopPreviewResize = useCallback(() => {
    resizeStateRef.current = null
    document.body.style.removeProperty('user-select')
  }, [])

  const handlePreviewResize = useCallback((event: MouseEvent) => {
    const resizeState = resizeStateRef.current
    if (!resizeState) {
      return
    }

    const nextHeightPx = resizeState.startHeightPx + (resizeState.startY - event.clientY)
    setPreviewHeightPx(clampPreviewHeight(nextHeightPx, resizeState.editorHeightPx))
  }, [])

  useEffect(() => {
    const handleMouseUp = () => {
      stopPreviewResize()
    }

    window.addEventListener('mousemove', handlePreviewResize)
    window.addEventListener('mouseup', handleMouseUp)

    return () => {
      window.removeEventListener('mousemove', handlePreviewResize)
      window.removeEventListener('mouseup', handleMouseUp)
      stopPreviewResize()
    }
  }, [handlePreviewResize, stopPreviewResize])

  const startPreviewResize = useCallback((event: ReactMouseEvent<HTMLDivElement>) => {
    if (previewCollapsed) {
      return
    }

    const currentEditorHeightPx = layoutRef.current?.clientHeight ?? editorHeightPx

    resizeStateRef.current = {
      startY: event.clientY,
      startHeightPx: clampPreviewHeight(previewHeightPx, currentEditorHeightPx),
      editorHeightPx: currentEditorHeightPx,
    }
    document.body.style.userSelect = 'none'
    event.preventDefault()
  }, [editorHeightPx, previewCollapsed, previewHeightPx])

  const togglePreviewCollapsed = useCallback(() => {
    setPreviewCollapsed((current) => !current)
  }, [])

  if (status === 'loading') {
    return (
      <div className="flex h-full items-center justify-center text-sm text-stone-500">
        Loading project...
      </div>
    )
  }

  if (status === 'missing') {
    return (
      <div className="flex h-full items-center justify-center p-8">
        <div className="rounded-3xl border border-stone-200 bg-white px-8 py-10 text-center shadow-sm">
          <h2 className="font-serif text-3xl text-stone-900">Project not found</h2>
          <p className="mt-3 text-sm text-stone-600">
            The selected project could not be loaded from the local project catalog.
          </p>
        </div>
      </div>
    )
  }

  const resolvedEditorHeightPx = layoutRef.current?.clientHeight ?? editorHeightPx
  const previewShellHeightPx = previewCollapsed
    ? COLLAPSED_PREVIEW_HEIGHT_PX
    : clampPreviewHeight(previewHeightPx, resolvedEditorHeightPx)

  return (
    <div ref={layoutRef} className="flex h-full min-w-0 flex-col">
      <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden" style={{ minHeight: `${TOP_WORKSPACE_MIN_HEIGHT_PX}px` }}>
        <div className="min-w-0 flex-1 overflow-hidden">
          <FlowCanvas />
        </div>
        <NodeConfigPanel />
      </div>
      <section
        data-testid="preview-panel-shell"
        data-layout-state={previewCollapsed ? 'collapsed' : 'expanded'}
        className="shrink-0 border-t border-gray-200 bg-white"
        style={{ height: `${previewShellHeightPx}px` }}
      >
        <div
          role="separator"
          aria-label="Resize data preview"
          aria-orientation="horizontal"
          data-testid="preview-resize-handle"
          onMouseDown={startPreviewResize}
          className={`group flex h-3 items-center justify-center border-b border-gray-100 ${
            previewCollapsed ? 'cursor-default' : 'cursor-row-resize'
          }`}
        >
          <div className="h-1 w-16 rounded-full bg-stone-200 transition group-hover:bg-stone-300" />
        </div>
        <div className="flex h-[calc(100%-0.75rem)] min-h-0 flex-col">
          <div className={`flex h-8 shrink-0 items-center justify-between px-4 text-xs ${
            previewCollapsed ? '' : 'border-b border-gray-100 bg-gray-50'
          }`}>
            <div className="font-semibold uppercase tracking-[0.18em] text-stone-500">Data Preview</div>
            <button
              type="button"
              onClick={togglePreviewCollapsed}
              aria-label={previewCollapsed ? 'Expand data preview' : 'Collapse data preview'}
              className="rounded px-2 py-1 text-stone-500 transition hover:bg-stone-100 hover:text-stone-700"
            >
              {previewCollapsed ? 'Expand' : 'Collapse'}
            </button>
          </div>
          {!previewCollapsed && (
            <div className="min-h-0 flex-1">
              <DataPreviewPanel />
            </div>
          )}
        </div>
      </section>
      <NodeErrorDialog />
    </div>
  )
}
