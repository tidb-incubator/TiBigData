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

package io.tidb.bigdata.test;

import java.util.Map;
import java.util.stream.Collectors;

public class TableUtils {

  public static String getTableSqlWithAllTypes(String databaseName, String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  tinyint,\n"
            + "    c2  smallint,\n"
            + "    c3  mediumint,\n"
            + "    c4  int,\n"
            + "    c5  bigint,\n"
            + "    c6  char(10),\n"
            + "    c7  varchar(20),\n"
            + "    c8  tinytext,\n"
            + "    c9  mediumtext,\n"
            + "    c10 text,\n"
            + "    c11 longtext,\n"
            + "    c12 binary(20),\n"
            + "    c13 varbinary(20),\n"
            + "    c14 tinyblob,\n"
            + "    c15 mediumblob,\n"
            + "    c16 blob,\n"
            + "    c17 longblob,\n"
            + "    c18 float,\n"
            + "    c19 double,\n"
            + "    c20 decimal(6, 3),\n"
            + "    c21 date,\n"
            + "    c22 time,\n"
            + "    c23 datetime,\n"
            + "    c24 timestamp,\n"
            + "    c25 year,\n"
            + "    c26 boolean,\n"
            + "    c27 json,\n"
            + "    c28 enum ('1','2','3'),\n"
            + "    c29 set ('a','b','c'),\n"
            + "    PRIMARY KEY(c1),\n"
            + "    UNIQUE KEY(c2)\n"
            + ")",
        databaseName, tableName);
  }

  public static String toSqlProperties(Map<String, String> properties) {
    return properties
        .entrySet()
        .stream()
        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
        .collect(Collectors.joining(","));
  }
}
