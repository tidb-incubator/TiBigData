/*
 *
 * Copyright 2017 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.types;

import io.tidb.bigdata.tidb.codec.Codec.DateCodec;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.codec.CodecDataOutput;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import java.sql.Date;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.tikv.common.exception.ConvertNotSupportException;
import org.tikv.common.exception.ConvertOverflowException;

public class DateType extends AbstractDateTimeType {
  private static final LocalDate EPOCH = new LocalDate(0);
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeDate};

  private DateType(MySQLType tp) {
    super(tp);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public DateTimeZone getTimezone() {
    return Converter.getLocalTimezone();
  }

  @Override
  public Date getOriginDefaultValueNonNull(String value, long version) {
    return Converter.convertToDate(value);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlDate(value);
  }

  private Date convertToMysqlDate(Object value) throws ConvertNotSupportException {
    Date result;
    if (value instanceof Long) {
      result = new Date((Long) value);
    } else if (value instanceof String) {
      result = Date.valueOf((String) value);
    } else if (value instanceof Date) {
      result = (Date) value;
    } else if (value instanceof java.sql.Timestamp) {
      result = new Date(((java.sql.Timestamp) value).getTime());
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    Date dt = Converter.convertToDate(value);
    DateCodec.writeDateFully(cdo, dt, getTimezone());
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    Date dt = Converter.convertToDate(value);
    DateCodec.writeDateProto(cdo, dt, getTimezone());
  }

  @Override
  public String getName() {
    return "DATE";
  }

  public int getDays(LocalDate d) {
    // count how many days from EPOCH
    int days = Days.daysBetween(EPOCH, d).getDays();
    // if the timezone has negative offset, minus one day.
    if (getTimezone().getOffset(0) < 0) {
      days -= 1;
    }
    return days;
  }

  /** {@inheritDoc} */
  @Override
  protected Long decodeNotNull(int flag, CodecDataInput cdi) {
    LocalDate date = decodeDate(flag, cdi);

    if (date == null) {
      return null;
    }

    return (long) getDays(date);
  }

  @Override
  protected Date decodeNotNullForBatchWrite(int flag, CodecDataInput cdi) {
    LocalDate date = decodeDate(flag, cdi);

    if (date == null) {
      return null;
    }
    return new Date(date.toDate().getTime());
  }
}
