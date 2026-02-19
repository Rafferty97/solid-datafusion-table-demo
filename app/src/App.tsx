import { createMemo, createSignal } from 'solid-js'
import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from './createRecordSetView'
import { empty, read_file } from 'engine'
import 'solid-tabular/styles.css'

function App() {
  const [recordSet, setRecordSet] = createSignal(empty())
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

  const handleUpload = () => {
    const el = document.querySelector<HTMLInputElement>('#fileupload')!
    const file = el.files![0]!
    read_file(file, file.name.match(/\.([a-z]+)$/i)![1]).then(setRecordSet)
  }

  return (
    <div style={{ width: '100%', padding: '20px', display: 'flex', 'flex-direction': 'column' }}>
      <input id="fileupload" type="file" onChange={handleUpload} />
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
