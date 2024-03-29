/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.util;

import io.tidb.bigdata.tidb.columnar.datatypes.CHType;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeDate;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeDateTime;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeDecimal;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeFixedString;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeMyDate;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeMyDateTime;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeFloat32;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeFloat64;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeInt16;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeInt32;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeInt64;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeInt8;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeUInt16;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeUInt32;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeUInt64;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeNumber.CHTypeUInt8;
import io.tidb.bigdata.tidb.columnar.datatypes.CHTypeString;
import org.apache.commons.lang3.StringUtils;

public class CHTypeMapping {
  public static CHType parseType(String typeName) {
    if (typeName == null || typeName.isEmpty()) {
      throw new UnsupportedOperationException("Empty CH type!");
    }
    typeName = typeName.trim();
    switch (typeName) {
      case "UInt8":
        return new CHTypeUInt8();
      case "UInt16":
        return new CHTypeUInt16();
      case "UInt32":
        return new CHTypeUInt32();
      case "UInt64":
        return new CHTypeUInt64();
      case "Int8":
        return new CHTypeInt8();
      case "Int16":
        return new CHTypeInt16();
      case "Int32":
        return new CHTypeInt32();
      case "Int64":
        return new CHTypeInt64();
      case "Float32":
        return new CHTypeFloat32();
      case "Float64":
        return new CHTypeFloat64();
      case "Date":
        return new CHTypeDate();
      case "DateTime":
        return new CHTypeDateTime();
      case "MyDateTime":
        return new CHTypeMyDateTime();
      case "MyDate":
        return new CHTypeMyDate();
      case "String":
        return new CHTypeString();
    }
    if (typeName.startsWith("FixedString")) {
      String remain = StringUtils.removeStart(typeName, "FixedString");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        int length = Integer.parseInt(remain);
        return new CHTypeFixedString(length);
      } catch (NumberFormatException e) {
        throw new UnsupportedOperationException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("MyDateTime")) {
      return new CHTypeMyDateTime();
    }
    if (typeName.startsWith("Decimal")) {
      String remain = StringUtils.removeStart(typeName, "Decimal");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        String[] args = remain.split(",");
        int precision = Integer.parseInt(args[0]);
        int scale = Integer.parseInt(args[1]);
        return new CHTypeDecimal(precision, scale);
      } catch (Exception e) {
        throw new UnsupportedOperationException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("Nullable")) {
      String remain = StringUtils.removeStart(typeName, "Nullable");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      CHType type = parseType(remain);
      type.setNullable(true);
      return type;
    }
    throw new UnsupportedOperationException("Unsupported CH type: " + typeName);
  }
}
