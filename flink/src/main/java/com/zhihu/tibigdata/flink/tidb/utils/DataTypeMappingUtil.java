/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.flink.tidb.utils;

import static com.pingcap.tikv.types.MySQLType.TypeFloat;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class DataTypeMappingUtil {

  /**
   * a default mapping: TiKV DataType -> Flink DataType
   *
   * @param dataType TiKV DataType
   * @return Flink DataType
   */
  public static DataType mapToFlinkType(com.pingcap.tikv.types.DataType dataType) {
    switch (dataType.getClass().getSimpleName()) {
      case "IntegerType":
        return DataTypes.BIGINT();
      case "StringType":
      case "EnumType":
      case "JsonType":
        return DataTypes.STRING();
      case "BytesType":
        return DataTypes.BYTES();
      case "TimeType":
        return DataTypes.TIME();
      case "DateType":
        return DataTypes.DATE();
      case "TimestampType":
      case "DateTimeType":
        return DataTypes.TIMESTAMP();
      case "RealType":
        if (dataType.getType() == TypeFloat) {
          return DataTypes.FLOAT();
        } else {
          return DataTypes.DOUBLE();
        }
      case "DecimalType":
        return DataTypes.DECIMAL((int) dataType.getLength(), dataType.getDecimal());
      default:
        throw new IllegalArgumentException(
            String.format("can not map %s to flink datatype", dataType));
    }
  }


  /**
   * transform TiKV java object to Flink java object by given Flink Datatype
   *
   * @param object TiKV java object
   * @param dataType Flink datatype
   */
  public static Optional<Object> getObjectWithDataType(Object object, DataType dataType) {
    if (object == null) {
      return Optional.empty();
    }
    switch (dataType.getConversionClass().getSimpleName()) {
      case "Integer":
        object = (int) (long) getObjectWithDataType(object, DataTypes.BIGINT()).get();
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
          // convert string to LocalDateTime
          String timeString = (String) object;
          try {
            object = LocalDateTime.parse(timeString);
          } catch (Exception e) {
            object = Timestamp.valueOf(timeString).toLocalDateTime();
          }
        } else if (object instanceof Long) {
          object = new Timestamp((Long) object).toLocalDateTime();
        }
        break;
      case "LocalTime":
        if (object instanceof Long || object instanceof Integer) {
          object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
        }
        break;
      default:
        object = ConvertUtils.convert(object, dataType.getConversionClass());
    }
    return Optional.of(object);
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
      Object object = row.getField(i);
      if (object == null) {
        continue;
      }
      switch (object.getClass().getSimpleName()) {
        case "String":
          object = StringData.fromString(object.toString());
          break;
        case "BigDecimal":
          BigDecimal bigDecimal = (BigDecimal) object;
          object = DecimalData
              .fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
          break;
        case "LocalDate":
          LocalDate localDate = (LocalDate) object;
          object = (int) localDate.toEpochDay();
          break;
        case "LocalDateTime":
          object = TimestampData.fromLocalDateTime((LocalDateTime) object);
          break;
        case "LocalTime":
          LocalTime localTime = (LocalTime) object;
          object = (int) (localTime.toNanoOfDay() / (1000 * 1000));
          break;
        default:
          // pass code style
          break;
      }
      rowData.setField(i, object);
    }
    return Optional.of(rowData);
  }
}
