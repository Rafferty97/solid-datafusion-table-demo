import { createEffect, createSignal, type Accessor } from 'solid-js'
import { Field, Table, tableFromIPC } from 'apache-arrow'

export type RecordSet = Readonly<{
  schema: Uint8Array
  numRows: number
  getRows(start: number, end: number): Promise<Uint8Array>
}>

export type RecordSetView = Readonly<{
  columns: Field[]
  numRows: number
  getCellValue(row: number, column: string): any
}>

export type RecordSetViewOptions = Readonly<{
  /** The rows in the underlying table will be fetched in batches of `batchSize` rows. */
  batchSize?: number
  /** The number of rows to fetch either side of the visible range. */
  overscan?: number
}>

export function createRecordSetView(
  data: RecordSet,
  visibleRange: Accessor<{ start: number; end: number }>,
  options: RecordSetViewOptions = {},
): RecordSetView {
  const batchSize = options.batchSize ?? 50
  const overscan = options.overscan ?? 20

  const schema = tableFromIPC(data.schema).schema
  const columns = schema.fields
  const numRows = data.numRows

  const [pages, setPages] = createSignal(new Map<number, Table>())
  let startPage = 0
  let endPage = 0

  const getCellValue = (row: number, column: string) => {
    const pageIdx = Math.floor(row / batchSize)
    const pageRow = row - pageIdx * batchSize
    return pages().get(pageIdx)?.get(pageRow)?.[column]
  }

  createEffect(() => {
    const { start, end } = visibleRange()
    const [prevStartPage, prevEndPage] = [startPage, endPage]

    // Compute new visible page range
    startPage = Math.floor(Math.max(start - overscan, 0) / batchSize)
    endPage = Math.ceil(Math.min(end + overscan, numRows) / batchSize)

    // Fetch pages that need to be fetched
    for (let pageIdx = startPage; pageIdx < endPage; pageIdx++) {
      if (pageIdx >= prevStartPage && pageIdx < prevEndPage) continue
      data
        .getRows(pageIdx * batchSize, (pageIdx + 1) * batchSize)
        .then(rowData => tableFromIPC([data.schema, rowData]))
        .then(table => setPages(m => new Map([...m, [pageIdx, table]])))
    }

    // Clean up pages that are no longer needed
    setPages(m => new Map([...m].filter(([i]) => i >= startPage && i < endPage)))
  })

  return { columns, numRows, getCellValue }
}
