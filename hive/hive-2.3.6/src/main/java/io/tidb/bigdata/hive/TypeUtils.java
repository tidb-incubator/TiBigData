/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.hive;

import static java.lang.String.format;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.tikv.common.types.DataType;
import org.tikv.common.types.StringType;

public class TypeUtils {

  public static PrimitiveObjectInspector toObjectInspector(DataType dataType) {
    boolean unsigned = dataType.isUnsigned();
    int precision = (int) dataType.getLength();
    int scale = 0;
    PrimitiveCategory primitiveCategory;
    switch (dataType.getType()) {
      case TypeBit:
        primitiveCategory = PrimitiveCategory.BOOLEAN;
        break;
      case TypeTiny:
      case TypeYear:
      case TypeShort:
        primitiveCategory = PrimitiveCategory.INT;
        break;
      case TypeInt24:
      case TypeLong:
        primitiveCategory = unsigned ? PrimitiveCategory.LONG : PrimitiveCategory.INT;
        break;
      case TypeLonglong:
        primitiveCategory = unsigned ? PrimitiveCategory.DECIMAL : PrimitiveCategory.LONG;
        break;
      case TypeFloat:
        primitiveCategory = PrimitiveCategory.FLOAT;
        break;
      case TypeDouble:
        primitiveCategory = PrimitiveCategory.DOUBLE;
        break;
      case TypeNull:
        primitiveCategory = PrimitiveCategory.VOID;
        break;
      case TypeDatetime:
      case TypeTimestamp:
        primitiveCategory = PrimitiveCategory.TIMESTAMP;
        break;
      case TypeDate:
      case TypeNewDate:
        primitiveCategory = PrimitiveCategory.DATE;
        break;
      case TypeTinyBlob:
      case TypeMediumBlob:
      case TypeLongBlob:
      case TypeBlob:
      case TypeVarString:
      case TypeString:
      case TypeVarchar:
        if (dataType instanceof StringType) {
          primitiveCategory = PrimitiveCategory.STRING;
          break;
        }
        primitiveCategory = PrimitiveCategory.BINARY;
        break;
      case TypeJSON:
      case TypeEnum:
      case TypeSet:
      case TypeDuration:
        primitiveCategory = PrimitiveCategory.STRING;
        break;
      case TypeDecimal:
      case TypeNewDecimal:
        primitiveCategory = PrimitiveCategory.DECIMAL;
        scale = dataType.getDecimal();
        break;
      case TypeGeometry:
      default:
        throw new IllegalArgumentException(
            format("Can not get hive type by tikv type: %s", dataType));
    }
    if (primitiveCategory == PrimitiveCategory.DECIMAL) {
      return new WritableHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale));
    } else {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveCategory);
    }
  }

  public static Writable toWriteable(Object object, DataType dataType) {
    if (object == null) {
      return NullWritable.get();
    }
    boolean unsigned = dataType.isUnsigned();
    switch (dataType.getType()) {
      case TypeBit:
        return new BooleanWritable(Integer.parseInt(object.toString()) == 1);
      case TypeTiny:
      case TypeYear:
      case TypeShort:
      case TypeInt24:
        return new IntWritable(Integer.parseInt(object.toString()));
      case TypeLong:
        return unsigned
            ? new LongWritable((long) object)
            : new IntWritable(Integer.parseInt(object.toString()));
      case TypeLonglong:
        return unsigned
            ? new HiveDecimalWritable(HiveDecimal.create((BigDecimal) object))
            : new LongWritable((long) object);
      case TypeFloat:
        return new FloatWritable((float) (double) object);
      case TypeDouble:
        return new DoubleWritable((double) object);
      case TypeNull:
        return NullWritable.get();
      case TypeDatetime:
      case TypeTimestamp:
        return new TimestampWritable(new Timestamp(((long) object) / 1000));
      case TypeDate:
      case TypeNewDate:
        return new DateWritable(
            Date.valueOf(LocalDate.ofEpochDay(Long.parseLong(object.toString()))));
      case TypeDuration:
        return new Text(LocalTime.ofNanoOfDay(Long.parseLong(object.toString())).toString());
      case TypeTinyBlob:
      case TypeMediumBlob:
      case TypeLongBlob:
      case TypeBlob:
      case TypeVarString:
      case TypeString:
      case TypeVarchar:
        if (dataType instanceof StringType) {
          return new Text(object.toString());
        }
        return new BytesWritable((byte[]) object);
      case TypeJSON:
      case TypeEnum:
      case TypeSet:
        return new Text(object.toString());
      case TypeDecimal:
      case TypeNewDecimal:
        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) object));
      case TypeGeometry:
      default:
        throw new IllegalArgumentException(
            format(
                "Can not covert tikv type to writable type, object = %s, type = %s",
                object, dataType));
    }
  }
}
