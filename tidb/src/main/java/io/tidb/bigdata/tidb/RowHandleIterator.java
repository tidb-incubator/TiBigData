package io.tidb.bigdata.tidb;

import com.google.common.base.Preconditions;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.IntHandle;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.ObjectRowImpl;
import io.tidb.bigdata.tidb.row.Row;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.tikv.common.util.Pair;

public interface RowHandleIterator extends Iterator<Pair<Row, Supplier<Handle>>> {

  class CoprocessorIteratorWrap implements RowHandleIterator {

    private final CoprocessorIterator<Row> coprocessorIterator;

    private CoprocessorIteratorWrap(CoprocessorIterator<Row> coprocessorIterator) {
      this.coprocessorIterator = coprocessorIterator;
    }

    @Override
    public boolean hasNext() {
      return coprocessorIterator.hasNext();
    }

    @Override
    public Pair<Row, Supplier<Handle>> next() {
      return new Pair<>(coprocessorIterator.next(), () -> null);
    }
  }

  static RowHandleIterator createWrap(CoprocessorIterator<Row> coprocessorIterator) {
    return new CoprocessorIteratorWrap(coprocessorIterator);
  }

  static RowHandleIterator create(
      TiTableInfo tiTableInfo,
      List<String> columns,
      List<String> queryColumns,
      CoprocessorIterator<Row> coprocessorIterator) {
    return new RowHandleIteratorImpl(tiTableInfo, columns, queryColumns, coprocessorIterator);
  }

  class RowHandleIteratorImpl implements RowHandleIterator {

    private final TiTableInfo tiTableInfo;
    private final List<String> columns;
    private final List<String> queryColumns;
    private final CoprocessorIterator<Row> coprocessorIterator;

    private RowHandleIteratorImpl(
        TiTableInfo tiTableInfo,
        List<String> columns,
        List<String> queryColumns,
        CoprocessorIterator<Row> coprocessorIterator) {
      this.tiTableInfo = tiTableInfo;
      this.columns = columns;
      this.queryColumns = queryColumns;
      this.coprocessorIterator = coprocessorIterator;
    }

    @Override
    public boolean hasNext() {
      return coprocessorIterator.hasNext();
    }

    @Override
    public Pair<Row, Supplier<Handle>> next() {
      Row row = coprocessorIterator.next();
      Row cutRow = cutRow(row);
      return new Pair<>(cutRow, () -> getHandle(row));
    }

    private Row cutRow(Row row) {
      Preconditions.checkArgument(
          row.fieldCount() == queryColumns.size(),
          "The length of row and queryColumns must be equals");
      Object[] values = new Object[columns.size()];
      for (int i = 0; i < values.length; i++) {
        values[i] = row.get(i, null);
      }
      return ObjectRowImpl.create(values);
    }

    // Use null to fill column value
    private Row toFullRow(Row row) {
      Object[] values = new Object[tiTableInfo.getColumns().size()];
      for (int i = 0; i < row.fieldCount(); i++) {
        String columnName = queryColumns.get(i);
        TiColumnInfo column = tiTableInfo.getColumn(columnName);
        values[column.getOffset()] = row.get(i, column.getType());
      }
      return ObjectRowImpl.create(values);
    }

    private Handle getHandle(Row row) {
      Row fullRow = toFullRow(row);
      if (tiTableInfo.getColumn(ClientSession.TIDB_ROW_ID_COLUMN_NAME) == null) {
        return TiDBEncodeHelper.extractHandle(
            fullRow, tiTableInfo, tiTableInfo.getPKIsHandleColumn());
      } else {
        TiColumnInfo rowId = tiTableInfo.getColumn(ClientSession.TIDB_ROW_ID_COLUMN_NAME);
        return new IntHandle(fullRow.getLong(rowId.getOffset()));
      }
    }
  }
}
