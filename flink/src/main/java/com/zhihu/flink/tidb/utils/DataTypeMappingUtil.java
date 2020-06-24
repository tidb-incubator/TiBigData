package com.zhihu.flink.tidb.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

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
      case "BytesType":
      case "EnumType":
      case "JsonType":
        return DataTypes.STRING();
      case "TimeType":
        return DataTypes.TIME();
      case "DateType":
        return DataTypes.DATE();
      case "TimestampType":
      case "DateTimeType":
        return DataTypes.TIMESTAMP();
      case "RealType":
        if (dataType.getType().name().equals("TypeFloat")) {
          return DataTypes.FLOAT();
        } else {
          return DataTypes.DOUBLE();
        }
      case "DecimalType":
        return DataTypes.DECIMAL((int) dataType.getLength(), dataType.getDecimal());
      default:
        throw new NullPointerException(String.format("can not map %s to flink datatype", dataType));
    }
  }


  /**
   * transform TiKV java object to Flink java object by given Flink Datatype
   *
   * @param object   TiKV java object
   * @param dataType Flink datatype
   * @return
   */
  public static Optional<Object> getObjectWithDataType(Object object, DataType dataType) {
    if (object == null) {
      return Optional.empty();
    }
    switch (dataType.toString()) {
      case "FLOAT":
        object = (float) (double) object;
        break;
      case "DATE":
        object = ((Date) object).toLocalDate();
        break;
      case "TIMESTAMP(6)":
        object = ((Timestamp) object).toLocalDateTime();
        break;
      case "TIME(0)":
        object = LocalTime.ofNanoOfDay(((long) object));
        break;
      case "STRING":
        if (object instanceof byte[]) {
          object = new String((byte[]) object);
        }
        break;
      case "BOOLEAN":
        if (object instanceof Long) {
          object = (Long) object != 0;
        }
        break;
    }
    return Optional.of(object);
  }
}
