package com.zhihu.flink.tidb.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalTime;
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
  public static Object getObjectWithDataType(Object object, DataType dataType) {
    if (object == null) {
      return null;
    }
    switch (dataType.toString()) {
      case "FLOAT":
        return (float) (double) object;
      case "DATE":
        return ((Date) object).toLocalDate();
      case "TIMESTAMP(6)":
        return ((Timestamp) object).toLocalDateTime();
      case "TIME(0)":
        return LocalTime.ofNanoOfDay(((long) object));
      case "STRING":
        if (object instanceof byte[]) {
          return new String((byte[]) object);
        } else {
          return object;
        }
      case "BOOLEAN":
        switch ((int) (long) object) {
          case 0:
            return false;
          case 1:
            return true;
          default:
            throw new IllegalArgumentException();
        }
      default:
        return object;
    }
  }
}
