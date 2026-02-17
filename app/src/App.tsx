import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from 'arrow-table'
// import { makeTable, tableToIPC } from 'apache-arrow'
import { create_record_set } from 'engine'
import 'solid-tabular/styles.css'

const recordSet = await create_record_set(1, 21)

function App() {
  // const table = makeTable({
  //   A: new Int32Array([1, 2, 3]),
  //   B: new Int32Array([4, 5, 6]),
  //   C: new Int32Array([7, 8, 9]),
  // })

  // const raw = tableToIPC(table)

  const data = createRecordSetView({
    schema: recordSet.encode_schema(),
    numRows: recordSet.num_rows(),
    getRows: (start, end) => Promise.resolve(recordSet.encode_rows(start, end)),
  })

  const tableProps = createTableState([])

  return (
    <div style={{ width: '100%', padding: '20px' }}>
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
  )
}

export default App
