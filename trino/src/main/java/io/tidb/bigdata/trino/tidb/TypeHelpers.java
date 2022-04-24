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

package io.tidb.bigdata.trino.tidb;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.tidb.bigdata.trino.tidb.TypeHelper.doubleHelper;
import static io.tidb.bigdata.trino.tidb.TypeHelper.longHelper;
import static io.tidb.bigdata.trino.tidb.TypeHelper.sliceHelper;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.String.format;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import org.tikv.common.types.BytesType;
import org.tikv.common.types.DataType;
import org.tikv.common.types.EnumType;
import org.tikv.common.types.SetType;
import org.tikv.common.types.StringType;

public final class TypeHelpers {

  private static final Map<Type, String> SQL_TYPES =
      ImmutableMap.<Type, String>builder()
          .put(BOOLEAN, "boolean")
          .put(BIGINT, "bigint")
          .put(INTEGER, "integer")
          .put(SMALLINT, "smallint")
          .put(TINYINT, "tinyint")
          .put(DOUBLE, "double")
          .put(REAL, "float")
          .put(VARBINARY, "mediumblob")
          .put(DATE, "date")
          .build();

  private static final ConcurrentHashMap<DataType, Optional<TypeHelper>> TYPE_HELPERS =
      new ConcurrentHashMap<>();

  public static TypeHelper decimalHelper(
      DataType tidbType, io.trino.spi.type.DecimalType decimalType) {
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
    int decimal = type.getDecimal();
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
            TimestampType.createTimestampType(decimal),
            (recordCursorInternal, field) ->
                recordCursorInternal.getLong(field) + TimeZone.getDefault().getRawOffset() * 1000L,
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
            TimeType.createTimeType(decimal),
            (recordCursorInternal, field) -> recordCursorInternal.getLong(field) * 1000);
      case TypeJSON:
        return varcharHelper(type, VarcharType.createUnboundedVarcharType());
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
        int precision = (int) length + max(-decimal, 0);
        // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
        if (precision > Decimals.MAX_PRECISION) {
          return null;
        }
        return decimalHelper(type, createDecimalType(precision, max(decimal, 0)));
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

  public static String toSqlString(Type type) {
    if (type instanceof TimeWithTimeZoneType || type instanceof TimestampWithTimeZoneType) {
      throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
    if (type instanceof TimestampType) {
      return format("timestamp(%s)", ((TimestampType) type).getPrecision());
    }
    if (type instanceof VarcharType) {
      VarcharType varcharType = (VarcharType) type;
      if (varcharType.isUnbounded()) {
        return "longtext";
      }
      Integer length = varcharType.getLength().orElseThrow(IllegalStateException::new);
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
      int length = ((CharType) type).getLength();
      if (length <= 255) {
        return "char(" + length + ")";
      }
      return "text";
    }
    if (type instanceof DecimalType) {
      return format(
          "decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
    }
    if (type instanceof TimeType) {
      return format("time(%s)", ((TimeType) type).getPrecision());
    }
    String sqlType = SQL_TYPES.get(type);
    if (sqlType != null) {
      return sqlType;
    }
    return type.getDisplayName();
  }
}
