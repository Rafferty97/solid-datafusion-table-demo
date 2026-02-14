import { Table, createTableState } from 'solid-tabular'
import { createRecordSetView } from 'arrow-table'
import 'solid-tabular/styles.css'
import { makeTable, tableToIPC } from 'apache-arrow'

function App() {
  const table = makeTable({
    A: new Int32Array([1, 2, 3]),
    B: new Int32Array([4, 5, 6]),
    C: new Int32Array([7, 8, 9]),
  })

  const raw = tableToIPC(table)

  const data = createRecordSetView({
    schema: raw.slice(0, 232),
    numRows: 3,
    getRows: () => Promise.resolve(raw.slice(232)),
  })

  // const tableProps = createTableState([
  //   { A: 1, B: 2, C: 3 },
  //   { A: 4, B: 5, C: 6 },
  //   { A: 7, B: 8, C: 9 },
  // ])

  return (
    <div style={{ width: '100%', padding: '20px' }}>
      <Table
        columns={data.columns.map(f => f.name)}
        numRows={data.numRows}
        getCellValue={(row, col) => data.getCellValue(row, col) ?? ''}
        onViewportChanged={data.setVisibleRange}
      />
    </div>
  )
}

export default App
