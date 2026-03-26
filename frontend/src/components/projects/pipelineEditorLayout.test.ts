import { describe, expect, it } from 'vitest'
import {
  clampPreviewHeight,
  DEFAULT_PREVIEW_HEIGHT_PX,
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
})
