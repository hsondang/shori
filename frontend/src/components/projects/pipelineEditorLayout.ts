export const DEFAULT_PREVIEW_HEIGHT_PX = 256
export const COLLAPSED_PREVIEW_HEIGHT_PX = 44
export const MIN_PREVIEW_HEIGHT_PX = 144
export const MAX_PREVIEW_HEIGHT_RATIO = 0.45
export const TOP_WORKSPACE_MIN_HEIGHT_PX = 240
export const NODE_CONFIG_PANEL_WIDTH_PX = 320
export const NODE_CONFIG_PANEL_MAX_WIDTH_PX = 704
export const NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH_PX = 448
export const NODE_CONFIG_PANEL_EXPANDED_DEFAULT_WIDTH_RATIO = 0.36

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

export function getNodeConfigPanelWidthBounds(expanded: boolean) {
  return {
    min: expanded ? NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH_PX : NODE_CONFIG_PANEL_WIDTH_PX,
    max: NODE_CONFIG_PANEL_MAX_WIDTH_PX,
  }
}

export function clampNodeConfigPanelWidth(requestedWidthPx: number, expanded: boolean) {
  const { min, max } = getNodeConfigPanelWidthBounds(expanded)
  if (!Number.isFinite(requestedWidthPx)) {
    return min
  }

  return Math.min(Math.max(requestedWidthPx, min), max)
}

export function getDefaultExpandedNodeConfigPanelWidth(viewportWidthPx: number) {
  if (!Number.isFinite(viewportWidthPx) || viewportWidthPx <= 0) {
    return NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH_PX
  }

  return clampNodeConfigPanelWidth(
    Math.round(viewportWidthPx * NODE_CONFIG_PANEL_EXPANDED_DEFAULT_WIDTH_RATIO),
    true
  )
}
