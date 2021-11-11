package io.tidb.bigdata.flink.connector.sink;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.ObjectRowImpl;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType;
import org.tikv.common.types.StringType;

public class TiDBRowConverter implements Serializable {

  private final TiTableInfo tiTableInfo;
  private final boolean ignoreAutoincrementColumn;
  private final List<TiColumnInfo> columns;

  public TiDBRowConverter(TiTableInfo tiTableInfo, boolean ignoreAutoincrementColumn) {
    this.ignoreAutoincrementColumn = ignoreAutoincrementColumn;
    this.tiTableInfo = tiTableInfo;
    this.columns = tiTableInfo.getColumns();
  }

  private void setDefaultValue(Row row, int pos, DataType type) {
    // TODO set default value
    row.set(pos, type, null);
  }

  public Row toTiRow(RowData rowData) {
    int arity = rowData.getArity();
    Row tiRow = ObjectRowImpl.create(arity);
    for (int i = 0; i < arity; i++) {
      TiColumnInfo tiColumnInfo = columns.get(i);
      DataType type = tiColumnInfo.getType();
      if (rowData.isNullAt(i)) {
        tiRow.setNull(i);
        // check not null
        if (type.isNotNull()) {
          if (type.isAutoIncrement()) {
            if (!ignoreAutoincrementColumn) {
              throw new IllegalStateException(
                  String.format("Auto increment column: %s can not be null",
                      tiColumnInfo.getName()));
            } else {
              continue;
            }
          }
          throw new IllegalStateException(
              String.format("Column: %s can not be null", tiColumnInfo.getName()));
        }
        continue;
      }
      boolean unsigned = type.isUnsigned();
      int length = (int) type.getLength();
      switch (type.getType()) {
        case TypeNull:
          break;
        case TypeBit:
        case TypeTiny:
        case TypeShort:
        case TypeInt24:
        case TypeLong:
        case TypeYear:
          tiRow.set(i, type, rowData.getLong(i));
          break;
        case TypeDatetime:
        case TypeTimestamp:
          tiRow.set(i, type, rowData.getTimestamp(i, 6).toTimestamp());
          break;
        case TypeNewDate:
        case TypeDate:
          tiRow.set(i, type, Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(i))));
          break;
        case TypeDuration:
          tiRow.set(i, type, 1000L * 1000L * rowData.getInt(i));
          break;
        case TypeLonglong:
          if (unsigned) {
            tiRow.set(i, type, rowData.getDecimal(i, length, 0).toBigDecimal());
          } else {
            tiRow.set(i, type, rowData.getLong(i));
          }
          break;
        case TypeFloat:
          tiRow.set(i, type, rowData.getFloat(i));
          break;
        case TypeDouble:
          tiRow.set(i, type, rowData.getDouble(i));
          break;
        case TypeTinyBlob:
        case TypeMediumBlob:
        case TypeLongBlob:
        case TypeBlob:
        case TypeVarString:
        case TypeString:
        case TypeVarchar:
          if (type instanceof StringType) {
            tiRow.set(i, type, rowData.getString(i).toString());
          } else {
            tiRow.set(i, type, rowData.getBinary(i));
          }
          break;
        case TypeSet:
        case TypeJSON:
          // since tikv java client is not supported json/set encoding
          throw new TiBatchWriteException("JSON/SET encoding is not supported now.");
        case TypeEnum:
          tiRow.set(i, type, rowData.getString(i).toString());
          break;
        case TypeDecimal:
        case TypeNewDecimal:
          tiRow.set(i, type, rowData.getDecimal(i, length, type.getDecimal()).toBigDecimal());
          break;
        case TypeGeometry:
        default:
          throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
      }
    }
    return tiRow;
  }


}
