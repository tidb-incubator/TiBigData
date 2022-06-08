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

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.connector.TiDBOptions.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.ClassRule;

public abstract class FlinkTestBase {

  @ClassRule public static final TiDBTestDatabase testDatabase = new TiDBTestDatabase();

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "tiflink_test";

  public static final String CREATE_DATABASE_SQL =
      String.format("CREATE DATABASE IF NOT EXISTS `%s`", DATABASE_NAME);

  protected static final String TABLE_WITHOUT_INDEX =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint,\n"
          + "    c2  bigint,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    c5  bigint,\n"
          + "    c6  longtext,\n"
          + "    c7  longtext,\n"
          + "    c8  longtext,\n"
          + "    c9  longtext,\n"
          + "    c10 longtext,\n"
          + "    c11 longtext,\n"
          + "    c12 float,\n"
          + "    c13 double,\n"
          + "    c14 date,\n"
          + "    c15 time,\n"
          + "    c16 datetime,\n"
          + "    c17 timestamp\n"
          + ")";

  protected static final String TABLE_WITH_INDEX =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint,\n"
          + "    c2  bigint,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    c5  bigint,\n"
          + "    c6  longtext,\n"
          + "    c7  longtext,\n"
          + "    c8  longtext,\n"
          + "    c9  longtext,\n"
          + "    c10 longtext,\n"
          + "    c11 longtext,\n"
          + "    c12 float,\n"
          + "    c13 double,\n"
          + "    c14 date,\n"
          + "    c15 time,\n"
          + "    c16 datetime,\n"
          + "    c17 timestamp,\n"
          + "    unique key(c1)\n"
          + ")";

  protected TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    return TableEnvironment.create(settings);
  }

  protected StreamTableEnvironment getBatchModeStreamTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    return tableEnvironment;
  }

  protected TiDBCatalog initTiDBCatalog(
      String dstTable,
      String createTableSql,
      TableEnvironment tableEnvironment,
      Map<String, String> properties) {
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, dstTable);
    String createTiDBSql = String.format(createTableSql, DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    return tiDBCatalog;
  }

  protected void generateData(String tableName, int rowCount) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(10000));
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, tableName);
    String createTiDBSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
                + "(\n"
                + "    c1  bigint,\n"
                + "    c2  bigint,\n"
                + "    c3  bigint,\n"
                + "    c4  bigint,\n"
                + "    c5  bigint,\n"
                + "    c6  longtext,\n"
                + "    c7  longtext,\n"
                + "    c8  longtext,\n"
                + "    c9  longtext,\n"
                + "    c10 longtext,\n"
                + "    c11 longtext,\n"
                + "    c12 float,\n"
                + "    c13 double,\n"
                + "    c14 date,\n"
                + "    c15 time,\n"
                + "    c16 datetime,\n"
                + "    c17 timestamp\n"
                + ")",
            DATABASE_NAME, tableName);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    CatalogBaseTable table = tiDBCatalog.getTable(DATABASE_NAME, tableName);
    String createDatagenSql =
        format(
            "CREATE TABLE datagen \n%s\n WITH (\n"
                + " 'connector' = 'datagen',\n"
                + " 'number-of-rows'='%s',\n"
                + " 'fields.c1.kind'='sequence',\n"
                + " 'fields.c1.start'='1',\n"
                + " 'fields.c1.end'='%s'\n"
                + ")",
            table.getUnresolvedSchema().toString(), rowCount, rowCount);
    tableEnvironment.executeSql(createDatagenSql);
    String sql =
        format("INSERT INTO `tidb`.`%s`.`%s` SELECT * FROM datagen", DATABASE_NAME, tableName);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    // splits
    String splitRegionSql =
        format(
            "SPLIT TABLE `%s`.`%s` BETWEEN (0) AND (%s) REGIONS %s",
            DATABASE_NAME, tableName, rowCount * 8, 100);
    tiDBCatalog.sqlUpdate(splitRegionSql);
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, tableName));
  }

  protected static void checkRowResult(
      TableEnvironment tableEnvironment, List<String> expected, String dstTable) {
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    CloseableIterator<Row> resultIterator = table.execute().collect();
    List<String> actualResult =
        Lists.newArrayList(resultIterator).stream().map(Row::toString).collect(Collectors.toList());

    Assert.assertEquals(expected, actualResult);
  }
}
