/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.mapreduce.tidb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Map;

public class TiDBResultSet implements ResultSet {
  private Object[] values;
  private ResultSetMetaData resultSetMetaData;

  public TiDBResultSet(int count, ResultSetMetaData resultSetMetaData) {
    this.values = new Object[count + 1];
    this.resultSetMetaData = resultSetMetaData;
  }

  public void setObject(Object object, int columnIndex) {
    this.values[columnIndex] = object;
  }

  @Override
  public String getString(int columnIndex) {
    if (null == values[columnIndex]) {
      return null;
    } else if (values[columnIndex] instanceof Date || values[columnIndex] instanceof Time
        || values[columnIndex] instanceof Timestamp) {
      return values[columnIndex].toString();
    }
    return (String) values[columnIndex];
  }

  @Override
  public String getString(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    if (null == values[columnIndex]) {
      return false;
    } else if (values[columnIndex] instanceof Long) {
      return ((Long) values[columnIndex]) > 0;
    }
    if (values[columnIndex] instanceof Integer) {
      return ((Integer) values[columnIndex]) > 0;
    }
    return (Boolean) values[columnIndex];
  }

  @Override
  public boolean getBoolean(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public byte getByte(int columnIndex) {
    return null == values[columnIndex] ? (byte) 0 : (Byte) values[columnIndex];
  }

  @Override
  public byte getByte(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public short getShort(int columnIndex) {
    return null == values[columnIndex] ? (short) 0 : (Short) values[columnIndex];
  }

  @Override
  public short getShort(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public int getInt(int columnIndex) {
    if (null == values[columnIndex]) {
      return 0;
    } else if (values[columnIndex] instanceof Long) {
      return ((Long) values[columnIndex]).intValue();
    } else if (values[columnIndex] instanceof Boolean) {
      return ((Boolean) values[columnIndex]) ? 1 : 0;
    }
    return (Integer) values[columnIndex];
  }

  @Override
  public int getInt(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public long getLong(int columnIndex) {
    return null == values[columnIndex] ? (long) 0 : (Long) values[columnIndex];
  }

  @Override
  public long getLong(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public float getFloat(int columnIndex) {
    if (null == values[columnIndex]) {
      return (float) 0;
    } else if (values[columnIndex] instanceof Double) {
      return ((Double) values[columnIndex]).floatValue();
    }
    return (Float) values[columnIndex];
  }

  @Override
  public float getFloat(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public double getDouble(int columnIndex) {
    return null == values[columnIndex] ? (double) 0 : (Double) values[columnIndex];
  }

  @Override
  public double getDouble(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    return null == values[columnIndex] ? null : (byte[]) values[columnIndex];
  }

  @Override
  public byte[] getBytes(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Date getDate(int columnIndex) {
    if (null == values[columnIndex]) {
      return null;
    } else if (values[columnIndex] instanceof Long) {
      return Date.valueOf(LocalDate.ofEpochDay((Long) values[columnIndex]));
    }
    return (Date) values[columnIndex];
  }

  @Override
  public Date getDate(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Time getTime(int columnIndex) {
    if (null == values[columnIndex]) {
      return null;
    } else if (values[columnIndex] instanceof Long) {
      return Time.valueOf(LocalTime.ofNanoOfDay((Long) values[columnIndex]));
    }
    return (Time) values[columnIndex];
  }

  @Override
  public Time getTime(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    if (null == values[columnIndex]) {
      return null;
    } else if (values[columnIndex] instanceof Long) {
      return new Timestamp((Long) values[columnIndex] / 1000);
    }
    return (Timestamp) values[columnIndex];
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) {
    return null == values[columnIndex] ? null : (BigDecimal) values[columnIndex];
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return resultSetMetaData;
  }

  @Override
  public boolean next() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void close() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean wasNull() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public SQLWarning getWarnings() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void clearWarnings() {

  }

  @Override
  public String getCursorName() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public int findColumn(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isBeforeFirst() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isAfterLast() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isFirst() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isLast() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void beforeFirst() {

  }

  @Override
  public void afterLast() {

  }

  @Override
  public boolean first() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean last() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public int getRow() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean absolute(int row) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean relative(int rows) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean previous() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void setFetchDirection(int direction) {

  }

  @Override
  public int getFetchDirection() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void setFetchSize(int rows) {

  }

  @Override
  public int getFetchSize() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public int getType() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public int getConcurrency() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean rowUpdated() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean rowInserted() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean rowDeleted() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) {

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) {

  }

  @Override
  public void updateByte(int columnIndex, byte x) {

  }

  @Override
  public void updateByte(String columnLabel, byte x) {

  }

  @Override
  public void updateShort(int columnIndex, short x) {

  }

  @Override
  public void updateShort(String columnLabel, short x) {

  }

  @Override
  public void updateInt(int columnIndex, int x) {

  }

  @Override
  public void updateInt(String columnLabel, int x) {

  }

  @Override
  public void updateLong(int columnIndex, long x) {

  }

  @Override
  public void updateLong(String columnLabel, long x) {

  }

  @Override
  public void updateFloat(int columnIndex, float x) {

  }

  @Override
  public void updateFloat(String columnLabel, float x) {

  }

  @Override
  public void updateDouble(int columnIndex, double x) {

  }

  @Override
  public void updateDouble(String columnLabel, double x) {

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) {

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) {

  }

  @Override
  public void updateString(int columnIndex, String x) {

  }

  @Override
  public void updateString(String columnLabel, String x) {

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) {

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) {

  }

  @Override
  public void updateDate(int columnIndex, Date x) {

  }

  @Override
  public void updateDate(String columnLabel, Date x) {

  }

  @Override
  public void updateTime(int columnIndex, Time x) {

  }

  @Override
  public void updateTime(String columnLabel, Time x) {

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) {

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) {

  }

  @Override
  public void updateNull(String columnLabel) {

  }

  @Override
  public void updateNull(int columnIndex) {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) {

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) {

  }

  @Override
  public void updateObject(int columnIndex, Object x) {

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) {

  }

  @Override
  public void updateObject(String columnLabel, Object x) {

  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) {

  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) {

  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) {

  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType) {

  }

  @Override
  public void insertRow() {

  }

  @Override
  public void updateRow() {

  }

  @Override
  public void deleteRow() {

  }

  @Override
  public void refreshRow() {

  }

  @Override
  public void cancelRowUpdates() {

  }

  @Override
  public void moveToInsertRow() {

  }

  @Override
  public void moveToCurrentRow() {

  }

  @Override
  public Statement getStatement() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Ref getRef(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Ref getRef(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Blob getBlob(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Blob getBlob(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Clob getClob(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Clob getClob(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Array getArray(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Array getArray(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Object getObject(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Object getObject(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public URL getURL(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public URL getURL(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) {

  }

  @Override
  public void updateRef(String columnLabel, Ref x) {

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) {

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) {

  }

  @Override
  public void updateClob(int columnIndex, Clob x) {

  }

  @Override
  public void updateClob(String columnLabel, Clob x) {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) {

  }

  @Override
  public void updateArray(int columnIndex, Array x) {

  }

  @Override
  public void updateArray(String columnLabel, Array x) {

  }

  @Override
  public RowId getRowId(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public RowId getRowId(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) {

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) {

  }

  @Override
  public int getHoldability() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isClosed() {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateNString(int columnIndex, String nstring) {

  }

  @Override
  public void updateNString(String columnLabel, String nstring) {

  }

  @Override
  public void updateNClob(int columnIndex, NClob nclob) {

  }

  @Override
  public void updateNClob(String columnLabel, NClob nclob) {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) {

  }

  @Override
  public NClob getNClob(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public NClob getNClob(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) {

  }

  @Override
  public String getNString(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public String getNString(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) {

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) {

  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    throw new IllegalArgumentException("no function");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    throw new IllegalArgumentException("no function");
  }
}
