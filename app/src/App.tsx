import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from './createRecordSetView'
import { create_record_set } from 'engine'
import 'solid-tabular/styles.css'

const recordSet = await create_record_set(1, 10001)

function App() {
  const data = createRecordSetView({
    schema: recordSet.encode_schema(),
    numRows: recordSet.num_rows(),
    getRows: (start, end) => Promise.resolve(recordSet.encode_rows(start, end)),
  })

  const tableProps = createTableState([])

  return (
    <div style={{ width: '100%', padding: '20px' }}>
      <div style={{ 'border-radius': '6px', overflow: 'hidden', height: '100%' }}>
        <Table
          columns={data.columns.map(f => f.name)}
          numRows={data.numRows}
          getCellValue={(row, col) => data.getCellValue(row, col) ?? ''}
          onViewportChanged={data.setVisibleRange}
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
