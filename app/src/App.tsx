import { createMemo, createSignal } from 'solid-js'
import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from './createRecordSetView'
import { infer_file_format, infer_file_schema, Plan, RecordSet, Schema, type FileFormat } from 'engine'
import 'solid-tabular/styles.css'

function App() {
  const [recordSet, setRecordSet] = createSignal(RecordSet.empty())
  const [visibleRange, setVisibleRange] = createSignal({ start: 0, end: 0 })

  const data = createMemo(() => {
    return createRecordSetView(
      {
        schema: recordSet().encode_schema(),
        numRows: recordSet().num_rows(),
        getRows: (start, end) => Promise.resolve(recordSet().encode_rows(start, end)),
      },
      visibleRange,
    )
  })

  const tableProps = createTableState([])

  let file: File | undefined
  let format: FileFormat = { format: 'parquet' }
  let schema: Schema = Schema.empty()

  const handleUpload = async () => {
    const el = document.querySelector<HTMLInputElement>('#fileupload')!
    file = el.files![0]!
    format = await infer_file_format(file)
    schema = await infer_file_schema(file, format)
    refresh()
  }

  const refresh = () => {
    if (!file) return
    Plan.read_file(file, format, schema)
      .then(plan => plan.limit(0).collect())
      .then(setRecordSet)
  }

  return (
    <div style={{ width: '100%', padding: '20px', display: 'flex', 'flex-direction': 'column' }}>
      <div style={{ display: 'flex' }}>
        <input id="fileupload" type="file" onChange={handleUpload} />
        <button onClick={refresh}>Refresh</button>
      </div>
      <div style={{ height: '20px' }} />
      <div style={{ 'border-radius': '6px', overflow: 'hidden', flex: '1' }}>
        <Table
          columns={data().columns.map(f => f.name)}
          numRows={data().numRows}
          getCellValue={(row, col) => data().getCellValue(row, col) ?? ''}
          onViewportChanged={(start, end) => setVisibleRange({ start, end })}
          activeRange={tableProps.activeRange}
          setActiveRange={tableProps.setActiveRange}
          getColumnSize={tableProps.getColumnSize}
          setColumnSize={tableProps.setColumnSize}
          resetColumnSize={tableProps.resetColumnSize}
          columnsResizeable
        />
      </div>
    </div>
  )
}

export default App
