import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import FlowCanvas from '../flow/FlowCanvas'
import NodeErrorDialog from '../flow/NodeErrorDialog'
import NodeConfigPanel from '../panels/NodeConfigPanel'
import DataPreviewPanel from '../panels/DataPreviewPanel'
import { usePipelineStore } from '../../store/pipelineStore'

export default function PipelineEditorPage() {
  const { projectId } = useParams()
  const pipelineId = usePipelineStore((s) => s.pipelineId)
  const loadPipeline = usePipelineStore((s) => s.loadPipeline)
  const [status, setStatus] = useState<'loading' | 'ready' | 'missing'>('loading')

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

  return (
    <div className="flex h-full min-w-0 flex-col">
      <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden">
        <div className="min-w-0 flex-1">
          <FlowCanvas />
        </div>
        <NodeConfigPanel />
      </div>
      <DataPreviewPanel />
      <NodeErrorDialog />
    </div>
  )
}
