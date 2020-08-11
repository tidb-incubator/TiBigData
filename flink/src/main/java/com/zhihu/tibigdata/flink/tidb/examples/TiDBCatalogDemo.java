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

package com.zhihu.tibigdata.flink.tidb.examples;

import com.zhihu.tibigdata.flink.tidb.catalog.TiDBCatalog;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TiDBCatalogDemo {

  public static void main(String[] args) {
    // properties
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    final Properties properties = parameterTool.getProperties();
    final String databaseName = parameterTool.getRequired("tidb.database.name");
    final String tableName = parameterTool.getRequired("tidb.table.name");
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // register TiDBCatalog
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);
    tableEnvironment.useCatalog("tidb");
    // query and print
    String sql = String.format("SELECT * FROM `%s`.`%s` LIMIT 100", databaseName, tableName);
    tableEnvironment.executeSql(sql).print();
  }
}
