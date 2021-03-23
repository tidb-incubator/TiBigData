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

package io.tidb.bigdata.cdc;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class RowColumn {

  /**
   * Formatter for SQL string representation of a time value.
   */
  static final DateTimeFormatter SQL_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();
  /**
   * Formatter for SQL string representation of a timestamp value (without UTC timezone).
   */
  static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .append(SQL_TIME_FORMAT)
          .toFormatter();
  private final String name; // Column name
  private final Object value; // Value
  private final boolean whereHandle; // Where Handle
  private final Type type;
  private final long flags;
  private Optional<Object> coerced; // Coerced value according to it's type

  public RowColumn(final String name, final Object value, final boolean whereHandle, final int type,
      final long flags) {
    this.name = name;
    this.whereHandle = whereHandle;
    this.type = Type.findByCode(type);
    this.flags = flags;
    this.value = value;
    this.coerced = Optional.empty();
  }

  private static byte[] parseBinary(final String str) {
    return Base64.getDecoder().decode(str);
  }

  private static LocalDate parseDate(final String str) {
    return ISO_LOCAL_DATE.parse(str).query(TemporalQueries.localDate());
  }

  private static LocalTime parseTime(final String str) {
    return SQL_TIME_FORMAT.parse(str).query(TemporalQueries.localTime());
  }

  public long getFlags() {
    return flags;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public boolean isWhereHandle() {
    return whereHandle;
  }

  public Object getOriginalValue() {
    return value;
  }

  public Object getValue() {
    if (!coerced.isPresent()) {
      coerced = Optional.ofNullable(type.coerce(value));
    }
    return coerced.get();
  }

  public Class getJavaType() {
    return type.getJavaType();
  }

  private String asStringInternal() {
    if (getJavaType().equals(String.class)) {
      return (String) getValue();
    } else {
      return value.toString();
    }
  }

  private String asStringNullable() {
    if (value == null) {
      return null;
    }
    return asStringInternal();
  }

  private byte[] asBinaryInternal() {
    if (getJavaType().equals(byte[].class)) {
      return (byte[]) getValue();
    } else {
      return parseBinary(asStringInternal());
    }
  }

  private byte[] asBinaryNullable() {
    if (value == null) {
      return null;
    }
    return asBinaryInternal();
  }

  private Integer asIntegerInternal() {
    if (getJavaType().equals(Integer.class)) {
      return (Integer) getValue();
    } else {
      return Integer.parseInt(asStringInternal());
    }
  }

  private Integer asIntegerNullable() {
    if (value == null) {
      return null;
    }
    return asIntegerInternal();
  }

  @SuppressWarnings("unchecked")
  private <T> T safeAsType(Supplier<Boolean> test, Function<String, T> converter) {
    if (value == null) {
      return null;
    }
    if (test.get()) {
      return (T) getValue();
    } else {
      return converter.apply(asStringInternal());
    }
  }

  public boolean isBoolean() {
    return type.isBoolean();
  }

  public Boolean asBoolean() {
    return safeAsType(this::isBoolean, Boolean::parseBoolean);
  }

  public boolean isTinyInt() {
    return type.isTinyInt();
  }

  public Byte asTinyInt() {
    return safeAsType(this::isTinyInt, Byte::parseByte);
  }

  public boolean isSmallInt() {
    return type.isSmallInt();
  }

  public Short asSmallInt() {
    return safeAsType(this::isSmallInt, Short::parseShort);
  }

  public boolean isInt() {
    return type.isInt();
  }

  public Integer asInt() {
    return asIntegerNullable();
  }

  public boolean isFloat() {
    return type.isFloat();
  }

  public Float asFloat() {
    return safeAsType(this::isFloat, Float::parseFloat);
  }

  public boolean isDouble() {
    return type.isDouble();
  }

  public Double asDouble() {
    return safeAsType(this::isDouble, Double::parseDouble);
  }

  public boolean isNull() {
    return type.isNull();
  }

  public Object asNull() {
    return null;
  }

  public boolean isTimestamp() {
    return type.isTimestamp();
  }

  public TemporalAccessor asTimestamp() {
    return safeAsType(this::isTimestamp, SQL_TIMESTAMP_FORMAT::parse);
  }

  public boolean isBigInt() {
    return type.isBigInt();
  }

  public Long asBigInt() {
    return safeAsType(this::isBigInt, Long::parseLong);
  }

  public boolean isMediumInt() {
    return type.isMediumInt();
  }

  public Integer asMediumInt() {
    return asIntegerNullable();
  }

  public boolean isDate() {
    return type.isDate();
  }

  public LocalDate asDate() {
    return safeAsType(this::isDate, RowColumn::parseDate);
  }

  public boolean isTime() {
    return type.isTime();
  }

  public LocalTime asTime() {
    return safeAsType(this::isTime, RowColumn::parseTime);
  }

  public boolean isDateTime() {
    return type.isDateTime();
  }

  public TemporalAccessor asDateTime() {
    return safeAsType(this::isDateTime, SQL_TIMESTAMP_FORMAT::parse);
  }

  public boolean isYear() {
    return type.isYear();
  }

  public Short asYear() {
    return safeAsType(this::isYear, Short::parseShort);
  }

  public boolean isVarchar() {
    return type.isVarchar();
  }

  public String asVarchar() {
    return asStringNullable();
  }

  public boolean isVarbinary() {
    return type.isVarbinary();
  }

  public byte[] asVarbinary() {
    return asBinaryNullable();
  }

  public boolean isBit() {
    return type.isBit();
  }

  public Long asBit() {
    return safeAsType(this::isBit, Long::parseLong);
  }

  public boolean isJson() {
    return type.isJson();
  }

  public String asJson() {
    return asStringNullable();
  }

  public boolean isDecimal() {
    return type.isDecimal();
  }

  public BigDecimal asDecimal() {
    return safeAsType(this::isDecimal, BigDecimal::new);
  }

  public boolean isEnum() {
    return type.isEnum();
  }

  public Integer asEnum() {
    return safeAsType(this::isEnum, Integer::parseInt);
  }

  public boolean isSet() {
    return type.isSet();
  }

  public Integer asSet() {
    return safeAsType(this::isSet, Integer::parseInt);
  }

  public boolean isTinyText() {
    return type.isTinyText();
  }

  public String asTinyText() {
    return asStringNullable();
  }

  public boolean isTinyBlob() {
    return type.isTinyBlob();
  }

  public byte[] asTinyBlob() {
    return asBinaryNullable();
  }

  public boolean isMediumText() {
    return type.isMediumText();
  }

  public String asMediumText() {
    return asStringNullable();
  }

  public boolean isMediumBlob() {
    return type.isMediumBlob();
  }

  public byte[] asMediumBlob() {
    return asBinaryNullable();
  }

  public boolean isLongText() {
    return type.isLongText();
  }

  public String asLongText() {
    return asStringNullable();
  }

  public boolean isLongBlob() {
    return type.isLongBlob();
  }

  public byte[] asLongBlob() {
    return asBinaryNullable();
  }

  public boolean isText() {
    return type.isText();
  }

  public String asText() {
    return asStringNullable();
  }

  public boolean isBlob() {
    return type.isBlob();
  }

  public byte[] asBlob() {
    return asBinaryNullable();
  }

  public boolean isChar() {
    return type.isChar();
  }

  public String asChar() {
    return asStringNullable();
  }

  public boolean isBinary() {
    return type.isBinary();
  }

  public byte[] asBinary() {
    return asBinaryNullable();
  }

  public boolean isGeometry() {
    return type.isGeometry();
  }

  public Object asGeometry() {
    throw new IllegalStateException("Geometry is not supported at this time");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowColumn)) {
      return false;
    }

    final RowColumn other = (RowColumn) o;
    return Objects.equals(type, other.type)
        && Objects.equals(name, other.name)
        && Objects.equals(value, other.value)
        && Objects.equals(whereHandle, other.whereHandle)
        && Objects.equals(flags, other.flags);
  }

  public enum Type {
    TINYINT(1, Byte.class, Type::numberToByte),
    BOOL(1, Boolean.class),
    SMALLINT(2, Short.class, Type::numberToShort),
    INT(3, Integer.class, Type::numberToInt),
    FLOAT(4, Float.class, Type::numberToFloat),
    DOUBLE(5, Double.class, Type::numberToDouble),
    NULL(6, Void.class, Type::toNull),
    TIMESTAMP(7, TemporalAccessor.class, Type::stringToTemporal),
    BIGINT(8, Long.class, Type::numberToLong),
    MEDIUMINT(9, Integer.class, Type::numberToInt),
    DATE(10, LocalDate.class, Type::stringToLocalDate),
    TIME(11, LocalTime.class, Type::stringToLocalTime),
    DATETIME(12, TemporalAccessor.class, Type::stringToTemporal),
    YEAR(13, Short.class, Type::numberToShort),
    VARCHAR(15, String.class),
    VARBINARY(253, byte[].class, Type::stringToBinary),
    BIT(16, Long.class, Type::numberToLong),
    JSON(245, String.class),
    DECIMAL(246, BigDecimal.class, Type::stringToDecimal),
    ENUM(247, Integer.class),
    SET(248, Integer.class),
    TINYTEXT(249, String.class),
    TINYBLOB(249, byte[].class, Type::stringToBinary),
    MEDIUMTEXT(250, String.class),
    MEDIUMBLOB(250, byte[].class, Type::stringToBinary),
    LONGTEXT(251, String.class),
    LONGBLOB(251, byte[].class, Type::stringToBinary),
    TEXT(252, String.class),
    BLOB(252, byte[].class, Type::stringToBinary),
    CHAR(254, String.class),
    BINARY(254, byte[].class, Type::stringToBinary),
    GEOMETRY(255, Void.class, Type::toNull); /* Geometry is not supported at this time */

    private static final Map<Integer, Type> byId = new HashMap<>();

    static {
      for (Type t : Type.values()) {
        if (byId.containsKey(t.getCode())) {
          // we use the first definition for those types sharing the same code
          continue;
        }
        byId.put(t.getCode(), t);
      }
      // 14 map to DATE as well
      byId.put(14, DATE);
    }

    private final int code;
    private final Class javaType;
    private final Function<Object, Object> coercion;

    Type(final int code, final Class javaType, final Function<Object, Object> coercion) {
      this.code = code;
      this.javaType = javaType;
      this.coercion = coercion;
    }

    Type(int code, Class javaType) {
      this(code, javaType, null);
    }

    static Type findByCode(final int code) {
      Type type = byId.get(code);
      if (type == null) {
        throw new IllegalArgumentException("Unknown type code: " + code);
      }
      return type;
    }

    private static Object numberToByte(final Object from) {
      return ((Number) from).byteValue();
    }

    private static Object numberToShort(final Object from) {
      return ((Number) from).shortValue();
    }

    private static Object numberToInt(final Object from) {
      return ((Number) from).intValue();
    }

    private static Object numberToLong(final Object from) {
      return ((Number) from).longValue();
    }

    private static Object numberToFloat(final Object from) {
      return ((Number) from).floatValue();
    }

    private static Object numberToDouble(final Object from) {
      return ((Number) from).doubleValue();
    }

    private static Object toNull(final Object from) {
      return null;
    }

    private static Object stringToTemporal(final Object from) {
      return SQL_TIMESTAMP_FORMAT.parse((String) from);
    }

    private static Object stringToDecimal(final Object from) {
      return new BigDecimal((String) from);
    }

    private static Object stringToBinary(final Object from) {
      return Base64.getDecoder().decode((String) from);
    }

    private static Object stringToLocalDate(final Object from) {
      return ISO_LOCAL_DATE.parse((String) from).query(TemporalQueries.localDate());
    }

    private static Object stringToLocalTime(final Object from) {
      return SQL_TIME_FORMAT.parse((String) from).query(TemporalQueries.localTime());
    }

    public Class getJavaType() {
      return javaType;
    }

    public int getCode() {
      return code;
    }

    public Object coerce(final Object from) {
      if (from == null || from.getClass().equals(javaType)) {
        return from;
      }
      if (coercion != null) {
        return coercion.apply(from);
      } else {
        return from;
      }
    }

    public boolean isBoolean() {
      return this.equals(BOOL);
    }

    public boolean isTinyInt() {
      return this.equals(TINYINT);
    }

    public boolean isSmallInt() {
      return this.equals(SMALLINT);
    }

    public boolean isInt() {
      return this.equals(INT);
    }

    public boolean isFloat() {
      return this.equals(FLOAT);
    }

    public boolean isDouble() {
      return this.equals(DOUBLE);
    }

    public boolean isNull() {
      return this.equals(NULL);
    }

    public boolean isTimestamp() {
      return this.equals(TIMESTAMP);
    }

    public boolean isBigInt() {
      return this.equals(BIGINT);
    }

    public boolean isMediumInt() {
      return this.equals(MEDIUMINT);
    }

    public boolean isDate() {
      return this.equals(DATE);
    }

    public boolean isTime() {
      return this.equals(TIME);
    }

    public boolean isDateTime() {
      return this.equals(DATETIME);
    }

    public boolean isYear() {
      return this.equals(YEAR);
    }

    public boolean isVarchar() {
      return this.equals(VARCHAR);
    }

    public boolean isVarbinary() {
      return this.equals(VARBINARY);
    }

    public boolean isBit() {
      return this.equals(BIT);
    }

    public boolean isJson() {
      return this.equals(JSON);
    }

    public boolean isDecimal() {
      return this.equals(DECIMAL);
    }

    public boolean isEnum() {
      return this.equals(ENUM);
    }

    public boolean isSet() {
      return this.equals(SET);
    }

    public boolean isTinyText() {
      return this.equals(TINYTEXT);
    }

    public boolean isTinyBlob() {
      return this.equals(TINYBLOB);
    }

    public boolean isMediumText() {
      return this.equals(MEDIUMTEXT);
    }

    public boolean isMediumBlob() {
      return this.equals(MEDIUMBLOB);
    }

    public boolean isLongText() {
      return this.equals(LONGTEXT);
    }

    public boolean isLongBlob() {
      return this.equals(LONGBLOB);
    }

    public boolean isText() {
      return this.equals(TEXT);
    }

    public boolean isBlob() {
      return this.equals(BLOB);
    }

    public boolean isChar() {
      return this.equals(CHAR);
    }

    public boolean isBinary() {
      return this.equals(BINARY);
    }

    public boolean isGeometry() {
      return this.equals(GEOMETRY);
    }
  }

}
