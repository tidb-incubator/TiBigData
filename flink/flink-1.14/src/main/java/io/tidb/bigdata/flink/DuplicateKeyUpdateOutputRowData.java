package io.tidb.bigdata.flink;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

public class DuplicateKeyUpdateOutputRowData implements RowData {

  private final RowData rowData;
  private final int[] index;

  public DuplicateKeyUpdateOutputRowData(RowData rowData, int[] index) {
    this.rowData = rowData;
    this.index = index;
  }

  @Override
  public int getArity() {
    return index.length;
  }

  @Override
  public RowKind getRowKind() {
    return rowData.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    rowData.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int pos) {
    return rowData.isNullAt(index[pos]);
  }

  @Override
  public boolean getBoolean(int pos) {
    return rowData.getBoolean(index[pos]);
  }

  @Override
  public byte getByte(int pos) {
    return rowData.getByte(index[pos]);
  }

  @Override
  public short getShort(int pos) {
    return rowData.getShort(index[pos]);
  }

  @Override
  public int getInt(int pos) {
    return rowData.getInt(index[pos]);
  }

  @Override
  public long getLong(int pos) {
    return rowData.getLong(index[pos]);
  }

  @Override
  public float getFloat(int pos) {
    return rowData.getFloat(index[pos]);
  }

  @Override
  public double getDouble(int pos) {
    return rowData.getDouble(index[pos]);
  }

  @Override
  public StringData getString(int pos) {
    return rowData.getString(index[pos]);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return rowData.getDecimal(index[pos], precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return rowData.getTimestamp(index[pos], precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return rowData.getRawValue(index[pos]);
  }

  @Override
  public byte[] getBinary(int pos) {
    return rowData.getBinary(index[pos]);
  }

  @Override
  public ArrayData getArray(int pos) {
    return rowData.getArray(index[pos]);
  }

  @Override
  public MapData getMap(int pos) {
    return rowData.getMap(index[pos]);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return rowData.getRow(index[pos], numFields);
  }
}