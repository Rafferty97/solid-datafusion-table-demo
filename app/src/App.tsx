import { createMemo, createSignal } from 'solid-js'
import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from './createRecordSetView'
import { empty, read_csv } from 'engine'
import 'solid-tabular/styles.css'

function App() {
  const [recordSet, setRecordSet] = createSignal(empty())

  const data = createMemo(() =>
    createRecordSetView({
      schema: recordSet().encode_schema(),
      numRows: recordSet().num_rows(),
      getRows: (start, end) => Promise.resolve(recordSet().encode_rows(start, end)),
    }),
  )

  const tableProps = createTableState([])

  const handleUpload = () => {
    const el = document.querySelector<HTMLInputElement>('#fileupload')!
    const file = el.files![0]!
    file.arrayBuffer().then(console.log)
    read_csv(file).then(setRecordSet)
  }

  return (
    <div style={{ width: '100%', padding: '20px' }}>
      <input id="fileupload" type="file" onChange={handleUpload} />
      <div style={{ height: '20px' }} />
      <div style={{ 'border-radius': '6px', overflow: 'hidden', height: '100%' }}>
        <Table
          columns={data().columns.map(f => f.name)}
          numRows={data().numRows}
          getCellValue={(row, col) => data().getCellValue(row, col) ?? ''}
          onViewportChanged={data().setVisibleRange}
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
