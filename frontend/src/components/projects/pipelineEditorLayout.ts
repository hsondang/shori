export const DEFAULT_PREVIEW_HEIGHT_PX = 256
export const COLLAPSED_PREVIEW_HEIGHT_PX = 44
export const MIN_PREVIEW_HEIGHT_PX = 144
export const MAX_PREVIEW_HEIGHT_RATIO = 0.45
export const TOP_WORKSPACE_MIN_HEIGHT_PX = 240
export const NODE_CONFIG_PANEL_WIDTH_PX = 320
export const NODE_CONFIG_PANEL_EXPANDED_WIDTH = '36vw'
export const NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH = '28rem'
export const NODE_CONFIG_PANEL_EXPANDED_MAX_WIDTH = '44rem'

function getFallbackMaxPreviewHeight() {
  return Math.max(MIN_PREVIEW_HEIGHT_PX, DEFAULT_PREVIEW_HEIGHT_PX)
}

export function getPreviewHeightBounds(editorHeightPx: number) {
  if (!Number.isFinite(editorHeightPx) || editorHeightPx <= 0) {
    return {
      min: MIN_PREVIEW_HEIGHT_PX,
      max: getFallbackMaxPreviewHeight(),
    }
  }

  const maxByRatio = Math.floor(editorHeightPx * MAX_PREVIEW_HEIGHT_RATIO)
  const maxByTopWorkspace = editorHeightPx - TOP_WORKSPACE_MIN_HEIGHT_PX
  const max = Math.max(MIN_PREVIEW_HEIGHT_PX, Math.min(maxByRatio, maxByTopWorkspace))

  return {
    min: MIN_PREVIEW_HEIGHT_PX,
    max,
  }
}

export function clampPreviewHeight(requestedHeightPx: number, editorHeightPx: number) {
  const { min, max } = getPreviewHeightBounds(editorHeightPx)
  if (!Number.isFinite(requestedHeightPx)) {
    return Math.min(Math.max(DEFAULT_PREVIEW_HEIGHT_PX, min), max)
  }

  return Math.min(Math.max(requestedHeightPx, min), max)
}
