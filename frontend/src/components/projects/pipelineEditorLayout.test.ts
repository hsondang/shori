import { describe, expect, it } from 'vitest'
import {
  clampNodeConfigPanelWidth,
  clampPreviewHeight,
  DEFAULT_PREVIEW_HEIGHT_PX,
  getDefaultExpandedNodeConfigPanelWidth,
  getNodeConfigPanelWidthBounds,
  getPreviewHeightBounds,
} from './pipelineEditorLayout'

describe('pipelineEditorLayout', () => {
  it('uses the default preview height when editor height is unavailable', () => {
    expect(clampPreviewHeight(DEFAULT_PREVIEW_HEIGHT_PX, 0)).toBe(DEFAULT_PREVIEW_HEIGHT_PX)
  })

  it('limits preview height to 45 percent of the editor body', () => {
    expect(getPreviewHeightBounds(800)).toEqual({ min: 144, max: 360 })
    expect(clampPreviewHeight(420, 800)).toBe(360)
  })

  it('enforces the minimum preview height', () => {
    expect(clampPreviewHeight(80, 800)).toBe(144)
  })

  it('clamps node config panel widths using mode-specific minimums', () => {
    expect(getNodeConfigPanelWidthBounds(false)).toEqual({ min: 320, max: 704 })
    expect(getNodeConfigPanelWidthBounds(true)).toEqual({ min: 448, max: 704 })
    expect(clampNodeConfigPanelWidth(240, false)).toBe(320)
    expect(clampNodeConfigPanelWidth(240, true)).toBe(448)
    expect(clampNodeConfigPanelWidth(900, false)).toBe(704)
  })

  it('resolves the default expanded panel width from viewport width', () => {
    expect(getDefaultExpandedNodeConfigPanelWidth(0)).toBe(448)
    expect(getDefaultExpandedNodeConfigPanelWidth(1200)).toBe(448)
    expect(getDefaultExpandedNodeConfigPanelWidth(1600)).toBe(576)
    expect(getDefaultExpandedNodeConfigPanelWidth(2400)).toBe(704)
  })
})
