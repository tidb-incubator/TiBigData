/*
 * Copyright 2021 TiKV Project Authors.
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

package io.tidb.bigdata.tidb.expression;

import static io.tidb.bigdata.tidb.types.MySQLType.TypeBlob;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeDate;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeDatetime;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeDecimal;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeDouble;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeDuration;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeEnum;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeFloat;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeInt24;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeJSON;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeLong;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeLongBlob;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeLonglong;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeMediumBlob;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeNewDate;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeNewDecimal;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeNull;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeSet;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeShort;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeString;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeTimestamp;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeTiny;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeTinyBlob;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeVarString;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeVarchar;
import static io.tidb.bigdata.tidb.types.MySQLType.TypeYear;

import java.util.HashMap;
import java.util.Map;
import io.tidb.bigdata.tidb.types.MySQLType;

public class TypeBlocklist extends Blocklist {
  private static final Map<MySQLType, String> typeToMySQLMap = initialTypeMap();

  public TypeBlocklist(String typesString) {
    super(typesString);
  }

  private static HashMap<MySQLType, String> initialTypeMap() {
    HashMap<MySQLType, String> map = new HashMap<>();
    map.put(TypeDecimal, "decimal");
    map.put(TypeTiny, "tinyint");
    map.put(TypeShort, "smallint");
    map.put(TypeLong, "int");
    map.put(TypeFloat, "float");
    map.put(TypeDouble, "double");
    map.put(TypeNull, "null");
    map.put(TypeTimestamp, "timestamp");
    map.put(TypeLonglong, "bigint");
    map.put(TypeInt24, "mediumint");
    map.put(TypeDate, "date");
    map.put(TypeDuration, "time");
    map.put(TypeDatetime, "datetime");
    map.put(TypeYear, "year");
    map.put(TypeNewDate, "date");
    map.put(TypeVarchar, "varchar");
    map.put(TypeJSON, "json");
    map.put(TypeNewDecimal, "decimal");
    map.put(TypeEnum, "enum");
    map.put(TypeSet, "set");
    map.put(TypeTinyBlob, "tinytext");
    map.put(TypeMediumBlob, "mediumtext");
    map.put(TypeLongBlob, "longtext");
    map.put(TypeBlob, "text");
    map.put(TypeVarString, "varString");
    map.put(TypeString, "string");
    return map;
  }

  public boolean isUnsupportedType(MySQLType sqlType) {
    return isUnsupported(typeToMySQLMap.getOrDefault(sqlType, ""));
  }
}
