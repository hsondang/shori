import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import SheetPickerModal from './SheetPickerModal'
import { usePipelineStore } from '../../store/pipelineStore'

vi.mock('../../api/client', () => ({
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
  getCacheStatus: vi.fn(() => Promise.resolve({ nodes: {} })),
  deletePreprocessedCsvArtifact: vi.fn(() => Promise.resolve({ deleted: true })),
  closePreviewSession: vi.fn(() => Promise.resolve({ closed: true })),
}))

function seedHub({ withImportedOrders = false } = {}) {
  act(() => {
    usePipelineStore.setState({
      nodes: [
        {
          id: 'hub-1',
          type: 'excel_workbook',
          position: { x: 0, y: 0 },
          data: {
            label: 'Workbook',
            tableName: '',
            config: { file_path: '/tmp/wb.xlsx', original_filename: 'wb.xlsx', sheet_names: ['DS Active', 'Summary'] },
          },
        },
        ...(withImportedOrders
          ? [{
              id: 'sheet-existing',
              type: 'excel_source' as const,
              position: { x: 340, y: 0 },
              data: {
                label: 'DS Active',
                tableName: 'ds_active',
                config: { file_path: '/tmp/wb.xlsx', original_filename: 'wb.xlsx', sheet_names: ['DS Active', 'Summary'], selected_sheet: 'DS Active' },
              },
            }]
          : []),
      ],
      edges: withImportedOrders ? [{ id: 'e1', source: 'hub-1', target: 'sheet-existing' }] : [],
      sheetPickerHubId: 'hub-1',
    })
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  act(() => usePipelineStore.getState().newPipeline())
})

describe('SheetPickerModal', () => {
  it('renders nothing while closed', () => {
    render(<SheetPickerModal />)
    expect(screen.queryByRole('dialog')).toBeNull()
  })

  it('prefills a slugified, deduped table name when a sheet is checked', async () => {
    const user = userEvent.setup()
    seedHub({ withImportedOrders: true })
    render(<SheetPickerModal />)

    expect(screen.getByText('imported (1)')).toBeInTheDocument()

    await user.click(screen.getByLabelText('Import sheet DS Active'))
    // 'ds_active' is taken by the existing sheet node → deduped suffix.
    expect(screen.getByLabelText('Table name')).toHaveValue('ds_active_2')
  })

  it('blocks confirm on an invalid table name and shows the error inline', async () => {
    const user = userEvent.setup()
    seedHub()
    render(<SheetPickerModal />)

    await user.click(screen.getByLabelText('Import sheet Summary'))
    const input = screen.getByLabelText('Table name')
    await user.clear(input)
    await user.type(input, '_shori_bad')

    expect(screen.getByText(/reserved/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /add 1 sheet/i })).toBeDisabled()
  })

  it('confirm dispatches addWorkbookSheets with the per-sheet options and closes', async () => {
    const user = userEvent.setup()
    const addWorkbookSheets = vi.fn().mockResolvedValue(['new-1'])
    seedHub()
    act(() => usePipelineStore.setState({ addWorkbookSheets }))
    render(<SheetPickerModal />)

    await user.click(screen.getByLabelText('Import sheet DS Active'))
    await user.type(screen.getByLabelText('Range (optional)'), 'A1:F500')
    await user.click(screen.getByLabelText('Read every column as text (all_varchar)'))
    await user.selectOptions(screen.getByLabelText('After creating'), 'in_memory')
    await user.click(screen.getByRole('button', { name: /add 1 sheet/i }))

    expect(addWorkbookSheets).toHaveBeenCalledWith(
      'hub-1',
      [{
        sheet: 'DS Active',
        tableName: 'ds_active',
        cellRange: 'A1:F500',
        header: true,
        allVarchar: true,
      }],
      { batchLoadMode: 'in_memory' },
    )
    expect(usePipelineStore.getState().sheetPickerHubId).toBeNull()
  })
})
