/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.prestodb.tidb;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeShortScaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.JsonType.JSON;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.tidb.bigdata.prestodb.tidb.TypeHelper.doubleHelper;
import static io.tidb.bigdata.prestodb.tidb.TypeHelper.longHelper;
import static io.tidb.bigdata.prestodb.tidb.TypeHelper.sliceHelper;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.String.format;
import static org.joda.time.DateTimeZone.UTC;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.types.BytesType;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.types.EnumType;
import io.tidb.bigdata.tidb.types.SetType;
import io.tidb.bigdata.tidb.types.StringType;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

public final class TypeHelpers {

  private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

  private static final Map<Type, String> SQL_TYPES =
      ImmutableMap.<Type, String>builder()
          .put(BOOLEAN, "boolean")
          .put(BIGINT, "bigint")
          .put(INTEGER, "integer")
          .put(SMALLINT, "smallint")
          .put(TINYINT, "tinyint")
          .put(DOUBLE, "double precision")
          .put(REAL, "real")
          .put(VARBINARY, "varbinary")
          .put(DATE, "date")
          .put(TIME, "time")
          .put(TIME_WITH_TIME_ZONE, "time with timezone")
          .put(TIMESTAMP, "timestamp")
          .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
          .build();

  private static final ConcurrentHashMap<DataType, Optional<TypeHelper>> TYPE_HELPERS =
      new ConcurrentHashMap<>();

  public static TypeHelper decimalHelper(
      DataType tidbType, com.facebook.presto.spi.type.DecimalType decimalType) {
    int scale = decimalType.getScale();
    if (decimalType.isShort()) {
      return longHelper(
          tidbType,
          decimalType,
          (cursor, columnIndex) ->
              encodeShortScaledValue(cursor.getBigDecimal(columnIndex), scale));
    }
    return sliceHelper(
        tidbType,
        decimalType,
        (cursor, columnIndex) -> encodeScaledValue(cursor.getBigDecimal(columnIndex), scale),
        s ->
            new BigDecimal(
                decodeUnscaledValue(s), scale, new MathContext(decimalType.getPrecision())));
  }

  public static TypeHelper varcharHelper(DataType tidbType, VarcharType varcharType) {
    return sliceHelper(
        tidbType, varcharType, (cursor, columnIndex) -> utf8Slice(cursor.getString(columnIndex)));
  }

  private static TypeHelper getHelperInternal(DataType type) {
    boolean unsigned = type.isUnsigned();
    long length = type.getLength();
    switch (type.getType()) {
      case TypeBit:
        return longHelper(type, TINYINT, RecordCursorInternal::getByte);
      case TypeTiny:
        return unsigned
            ? longHelper(type, SMALLINT, RecordCursorInternal::getShort)
            : longHelper(type, TINYINT, RecordCursorInternal::getByte);
      case TypeYear:
      case TypeShort:
        return unsigned
            ? longHelper(type, INTEGER, RecordCursorInternal::getInteger)
            : longHelper(type, SMALLINT, RecordCursorInternal::getShort);
      case TypeInt24:
        // FALLTHROUGH
      case TypeLong:
        return unsigned
            ? longHelper(type, BIGINT, RecordCursorInternal::getLong)
            : longHelper(type, INTEGER, RecordCursorInternal::getInteger);
      case TypeFloat:
        return longHelper(
            type,
            REAL,
            (cursor, column) -> floatToRawIntBits(cursor.getFloat(column)),
            l -> intBitsToFloat(l.intValue()));
      case TypeDouble:
        return doubleHelper(type, DOUBLE, RecordCursorInternal::getDouble);
      case TypeNull:
        return null;
      case TypeDatetime:
        // FALLTHROUGH
      case TypeTimestamp:
        return longHelper(
            type,
            TIMESTAMP,
            (recordCursorInternal, field) -> recordCursorInternal.getLong(field) / 1000,
            Timestamp::new);
      case TypeLonglong:
        return unsigned
            ? decimalHelper(type, createDecimalType((int) length, 0))
            : longHelper(type, BIGINT, RecordCursorInternal::getLong);
      case TypeDate:
        // FALLTHROUGH
      case TypeNewDate:
        return longHelper(
            type,
            DATE,
            RecordCursorInternal::getLong,
            days -> Date.valueOf(LocalDate.ofEpochDay(days)));
      case TypeDuration:
        return longHelper(
            type,
            TIME,
            (cursor, columnIndex) -> {
              long localMillis = cursor.getLong(columnIndex) / 1000000L;
              DateTimeZone zone = ISOChronology.getInstance().getZone();
              return zone.convertUTCToLocal(zone.getMillisKeepLocal(UTC, localMillis));
            },
            millis -> (1000000L * (((long) millis) + 8 * 3600 * 1000L)));
      case TypeJSON:
        return sliceHelper(
            type, JSON, (cursor, columnIndex) -> utf8Slice(cursor.getString(columnIndex)));
      case TypeSet:
        // FALLTHROUGH
      case TypeTinyBlob:
        // FALLTHROUGH
      case TypeMediumBlob:
        // FALLTHROUGH
      case TypeLongBlob:
        // FALLTHROUGH
      case TypeBlob:
        // FALLTHROUGH
      case TypeEnum:
        // FALLTHROUGH
      case TypeVarString:
        // FALLTHROUGH
      case TypeString:
        // FALLTHROUGH
      case TypeVarchar:
        // FALLTHROUGH
        if (type instanceof StringType || type instanceof SetType || type instanceof EnumType) {
          if (length > (long) VarcharType.MAX_LENGTH || length < 0) {
            return varcharHelper(type, VarcharType.createUnboundedVarcharType());
          }
          return varcharHelper(type, VarcharType.createVarcharType((int) length));
        } else if (type instanceof BytesType) {
          return sliceHelper(
              type,
              VARBINARY,
              (cursor, columnIndex) -> wrappedBuffer(cursor.getBytes(columnIndex)),
              Slice::getBytes);
        } else {
          return null;
        }
      case TypeDecimal:
        // FALLTHROUGH
      case TypeNewDecimal:
        int decimalDigits = type.getDecimal();
        int precision =
            (int) length
                + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
        if (precision > Decimals.MAX_PRECISION) {
          return null;
        }
        return decimalHelper(type, createDecimalType(precision, max(decimalDigits, 0)));
      case TypeGeometry:
        // FALLTHROUGH
      default:
        return null;
    }
  }

  public static Optional<TypeHelper> getHelper(DataType type) {
    Optional<TypeHelper> helper = TYPE_HELPERS.get(type);
    if (helper != null) {
      return helper;
    }
    helper = Optional.ofNullable(getHelperInternal(type));
    TYPE_HELPERS.putIfAbsent(type, helper);
    return helper;
  }

  public static Optional<Type> getPrestoType(DataType type) {
    return getHelper(type).map(TypeHelper::getPrestoType);
  }

  // copy from com.facebook.presto.plugin.mysql.MySqlClient#toSqlType
  public static String toSqlString(Type type) {
    if (REAL.equals(type)) {
      return "float";
    }
    if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
      throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
    if (TIMESTAMP.equals(type)) {
      return "datetime";
    }
    if (VARBINARY.equals(type)) {
      return "mediumblob";
    }
    if (isVarcharType(type)) {
      VarcharType varcharType = (VarcharType) type;
      if (varcharType.isUnbounded()) {
        return "longtext";
      }
      int length = varcharType.getLengthSafe();
      if (length <= 255) {
        return "tinytext";
      }
      if (length <= 65535) {
        return "text";
      }
      if (length <= 16777215) {
        return "mediumtext";
      }
      return "longtext";
    }
    if (type instanceof CharType) {
      if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
        return "char";
      }
      return "char(" + ((CharType) type).getLength() + ")";
    }
    if (type instanceof DecimalType) {
      return format(
          "decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
    }
    String sqlType = SQL_TYPES.get(type);
    if (sqlType != null) {
      return sqlType;
    }
    return type.getDisplayName();
  }
}
