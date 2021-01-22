/*
 * Copyright 2020 TiKV Project Authors.
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

package org.tikv.bigdata.flink.tidb;

import static java.lang.String.format;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.tikv.common.types.StringType;

public class TypeUtils {

  /**
   * a default mapping: TiKV DataType -> Flink DataType
   *
   * @param dataType TiKV DataType
   * @return Flink DataType
   */
  public static DataType getFlinkType(org.tikv.common.types.DataType dataType) {
    boolean unsigned = dataType.isUnsigned();
    int length = (int) dataType.getLength();
    switch (dataType.getType()) {
      case TypeBit:
        return DataTypes.BOOLEAN();
      case TypeTiny:
        if (length == 1) {
          return DataTypes.BOOLEAN();
        }
        return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
      case TypeYear:
      case TypeShort:
        return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
      case TypeInt24:
      case TypeLong:
        return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
      case TypeLonglong:
        return unsigned ? DataTypes.DECIMAL(length, 0) : DataTypes.BIGINT();
      case TypeFloat:
        return DataTypes.FLOAT();
      case TypeDouble:
        return DataTypes.DOUBLE();
      case TypeNull:
        return DataTypes.NULL();
      case TypeDatetime:
      case TypeTimestamp:
        return DataTypes.TIMESTAMP();
      case TypeDate:
      case TypeNewDate:
        return DataTypes.DATE();
      case TypeDuration:
        return DataTypes.TIME();
      case TypeTinyBlob:
      case TypeMediumBlob:
      case TypeLongBlob:
      case TypeBlob:
      case TypeVarString:
      case TypeString:
      case TypeVarchar:
        if (dataType instanceof StringType) {
          return DataTypes.STRING();
        }
        return DataTypes.BYTES();
      case TypeJSON:
      case TypeEnum:
      case TypeSet:
        return DataTypes.STRING();
      case TypeDecimal:
      case TypeNewDecimal:
        return DataTypes.DECIMAL(length, dataType.getDecimal());
      case TypeGeometry:
      default:
        throw new IllegalArgumentException(
            format("can not get flink datatype by tikv type: %s", dataType));
    }
  }


  /**
   * transform TiKV java object to Flink java object by given Flink Datatype
   *
   * @param object TiKV java object
   * @param dataType Flink datatype
   */
  public static Optional<Object> getObjectWithDataType(Object object, DataType dataType,
      DateTimeFormatter formatter) {
    if (object == null) {
      return Optional.empty();
    }
    Class<?> conversionClass = dataType.getConversionClass();
    if (dataType.getConversionClass() == object.getClass()) {
      return Optional.of(object);
    }
    switch (conversionClass.getSimpleName()) {
      case "String":
        if (object instanceof byte[]) {
          object = new String((byte[]) object);
        } else if (object instanceof Timestamp) {
          Timestamp timestamp = (Timestamp) object;
          object = formatter == null ? timestamp.toString()
              : timestamp.toLocalDateTime().format(formatter);
        } else {
          object = object.toString();
        }
        break;
      case "Integer":
        object = (int) (long) getObjectWithDataType(object, DataTypes.BIGINT(), formatter).get();
        break;
      case "Long":
        if (object instanceof LocalDate) {
          object = ((LocalDate) object).toEpochDay();
        } else if (object instanceof LocalDateTime) {
          object = Timestamp.valueOf(((LocalDateTime) object)).getTime();
        } else if (object instanceof LocalTime) {
          object = ((LocalTime) object).toNanoOfDay();
        }
        break;
      case "LocalDate":
        if (object instanceof Date) {
          object = ((Date) object).toLocalDate();
        } else if (object instanceof String) {
          object = LocalDate.parse((String) object);
        } else if (object instanceof Long || object instanceof Integer) {
          object = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
        }
        break;
      case "LocalDateTime":
        if (object instanceof Timestamp) {
          object = ((Timestamp) object).toLocalDateTime();
        } else if (object instanceof String) {
          String timeString = (String) object;
          object = formatter == null ? LocalDateTime.parse(timeString)
              : LocalDateTime.parse(timeString, formatter);
        } else if (object instanceof Long) {
          object = new Timestamp(((Long) object) / 1000).toLocalDateTime();
        }
        break;
      case "LocalTime":
        if (object instanceof Long || object instanceof Integer) {
          object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
        }
        break;
      default:
        object = ConvertUtils.convert(object, conversionClass);
    }
    return Optional.of(object);
  }

  public static Optional<Object> getObjectWithDataType(Object object, DataType dataType) {
    return getObjectWithDataType(object, dataType, null);
  }

  /**
   * transform Row to GenericRowData
   */
  public static Optional<GenericRowData> toRowData(Row row) {
    if (row == null) {
      return Optional.empty();
    }
    GenericRowData rowData = new GenericRowData(row.getArity());
    for (int i = 0; i < row.getArity(); i++) {
      rowData.setField(i, toRowDataType(row.getField(i)));
    }
    return Optional.of(rowData);
  }

  /**
   * transform Row type to GenericRowData type
   */
  public static Object toRowDataType(Object object) {
    Object result = object;
    if (object == null) {
      return null;
    }
    switch (object.getClass().getSimpleName()) {
      case "String":
        result = StringData.fromString(object.toString());
        break;
      case "BigDecimal":
        BigDecimal bigDecimal = (BigDecimal) object;
        result = DecimalData
            .fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
        break;
      case "LocalDate":
        LocalDate localDate = (LocalDate) object;
        result = (int) localDate.toEpochDay();
        break;
      case "LocalDateTime":
        result = TimestampData.fromLocalDateTime((LocalDateTime) object);
        break;
      case "LocalTime":
        LocalTime localTime = (LocalTime) object;
        result = (int) (localTime.toNanoOfDay() / (1000 * 1000));
        break;
      default:
        // pass code style
        break;
    }
    return result;
  }
}
