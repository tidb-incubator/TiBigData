package io.tidb.bigdata.flink.connector.utils;

import io.tidb.bigdata.tidb.row.ObjectRowImpl;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.flink.types.RowKind;

public class TiRow implements Row {

  private final RowKind rowKind;
  private final ObjectRowImpl objectRowImpl;

  public TiRow(Object[] values, RowKind rowKind) {
    this.objectRowImpl = (ObjectRowImpl) ObjectRowImpl.create(values);
    this.rowKind = rowKind;
  }

  public TiRow(int fieldCount, RowKind rowKind) {
    this.objectRowImpl = (ObjectRowImpl) ObjectRowImpl.create(fieldCount);
    this.rowKind = rowKind;
  }

  public RowKind getRowKind() {
    return rowKind;
  }

  @Override
  public void setNull(int pos) {
    objectRowImpl.setNull(pos);
  }

  @Override
  public boolean isNull(int pos) {
    return objectRowImpl.isNull(pos);
  }

  @Override
  public void setFloat(int pos, float v) {
    objectRowImpl.setFloat(pos, v);
  }

  @Override
  public float getFloat(int pos) {
    return objectRowImpl.getFloat(pos);
  }

  @Override
  public void setDouble(int pos, double v) {
    objectRowImpl.setDouble(pos, v);
  }

  @Override
  public double getDouble(int pos) {
    return objectRowImpl.getDouble(pos);
  }

  @Override
  public void setInteger(int pos, int v) {
    objectRowImpl.setInteger(pos, v);
  }

  @Override
  public int getInteger(int pos) {
    return objectRowImpl.getInteger(pos);
  }

  @Override
  public void setShort(int pos, short v) {
    objectRowImpl.setShort(pos, v);
  }

  @Override
  public short getShort(int pos) {
    return objectRowImpl.getShort(pos);
  }

  @Override
  public void setLong(int pos, long v) {
    objectRowImpl.setLong(pos, v);
  }

  @Override
  public long getLong(int pos) {
    return objectRowImpl.getLong(pos);
  }

  @Override
  public long getUnsignedLong(int pos) {
    return objectRowImpl.getUnsignedLong(pos);
  }

  @Override
  public void setString(int pos, String v) {
    objectRowImpl.setString(pos, v);
  }

  @Override
  public String getString(int pos) {
    return objectRowImpl.getString(pos);
  }

  @Override
  public void setTime(int pos, Time v) {
    objectRowImpl.setTime(pos, v);
  }

  @Override
  public Date getTime(int pos) {
    return objectRowImpl.getTime(pos);
  }

  @Override
  public void setTimestamp(int pos, Timestamp v) {
    objectRowImpl.setTimestamp(pos, v);
  }

  @Override
  public Timestamp getTimestamp(int pos) {
    return objectRowImpl.getTimestamp(pos);
  }

  @Override
  public void setDate(int pos, Date v) {
    objectRowImpl.setDate(pos, v);
  }

  @Override
  public Date getDate(int pos) {
    return objectRowImpl.getDate(pos);
  }

  @Override
  public void setBytes(int pos, byte[] v) {
    objectRowImpl.setBytes(pos, v);
  }

  @Override
  public byte[] getBytes(int pos) {
    return objectRowImpl.getBytes(pos);
  }

  @Override
  public void set(int pos, DataType type, Object v) {
    objectRowImpl.set(pos, type, v);
  }

  @Override
  public Object get(int pos, DataType type) {
    return objectRowImpl.get(pos, type);
  }

  @Override
  public int fieldCount() {
    return objectRowImpl.fieldCount();
  }

  @Override
  public String toString() {
    return objectRowImpl.toString();
  }
}
