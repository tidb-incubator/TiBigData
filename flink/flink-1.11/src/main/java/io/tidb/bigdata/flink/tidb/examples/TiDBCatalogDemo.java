/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.flink.tidb.examples;

import io.tidb.bigdata.flink.tidb.TiDBCatalog;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class TiDBCatalogDemo {

  public static void main(String[] args) {
    // properties
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    final Map<String, String> properties = parameterTool.toMap();
    final String databaseName = parameterTool.getRequired("tidb.database.name");
    final String tableName = parameterTool.getRequired("tidb.table.name");
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    // register TiDBCatalog
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);
    // query and print
    String sql = String.format("SELECT * FROM `tidb`.`%s`.`%s` LIMIT 100", databaseName, tableName);
    System.out.println("Flink SQL: " + sql);
    TableResult tableResult = tableEnvironment.executeSql(sql);
    System.out.println("TableSchema: \n" + tableResult.getTableSchema());
    tableResult.print();
  }
}
