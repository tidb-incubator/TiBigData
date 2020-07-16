package com.zhihu.flink.tidb.utils;

import static com.pingcap.tikv.types.MySQLType.TypeFloat;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Optional;
import org.apache.commons.beanutils.ConvertUtils;
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
      case "LocalDate":
        object = ((Date) object).toLocalDate();
        break;
      case "LocalDateTime":
        object = ((Timestamp) object).toLocalDateTime();
        break;
      case "LocalTime":
        object = LocalTime.ofNanoOfDay(((long) object));
        break;
      default:
        object = ConvertUtils.convert(object, dataType.getConversionClass());
    }
    return Optional.of(object);
  }
}
