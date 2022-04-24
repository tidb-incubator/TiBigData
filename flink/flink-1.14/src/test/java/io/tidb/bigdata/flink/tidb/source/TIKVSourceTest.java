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

package io.tidb.bigdata.flink.tidb.source;

import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.test.TableUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TIKVSourceTest extends FlinkTestBase {

  public static final String CREATE_DATAGEN_TABLE_SQL =
      "CREATE TABLE datagen (\n"
          + " c1 int,\n"
          + " proctime as PROCTIME()\n"
          + ") WITH (\n"
          + " 'connector' = 'datagen',\n"
          + " 'rows-per-second'='10',\n"
          + " 'fields.c1.kind'='random',\n"
          + " 'fields.c1.min'='1',\n"
          + " 'fields.c1.max'='10',\n"
          + " 'number-of-rows'='10'\n"
          + ")";

  @Test
  public void testSnapshotRead() throws Exception {
    for (int i = 1; i <= 3; i++) {
      // insert
      Map<String, String> properties = ConfigUtils.defaultProperties();
      ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
      String tableName = RandomUtils.randomString();
      clientSession.sqlUpdate(
          String.format("CREATE TABLE `%s`.`%s` (`c1` int,`c2` int)", DATABASE_NAME, tableName),
          String.format("INSERT INTO `%s`.`%s` VALUES(1,1)", DATABASE_NAME, tableName));

      if (i == 1) {
        // get timestamp
        properties.put(
            ClientConfig.SNAPSHOT_TIMESTAMP,
            ZonedDateTime.now().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        // wait for 1 second, because we use client time rather than server time
        Thread.sleep(1000L);
      } else {
        // get version
        long version = clientSession.getSnapshotVersion().getVersion();
        properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
      }

      // update
      clientSession.sqlUpdate(
          String.format("UPDATE `%s`.`%s` SET c1 = 2 WHERE c1 =1", DATABASE_NAME, tableName));

      if (i == 3) {
        // get timestamp
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        properties.put(
            ClientConfig.SNAPSHOT_TIMESTAMP,
            zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
      }

      // read by version
      TableEnvironment tableEnvironment = getTableEnvironment();
      properties.put("type", "tidb");
      String createCatalogSql =
          format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
      tableEnvironment.executeSql(createCatalogSql);
      String queryTableSql =
          format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME, tableName);

      try (CloseableIterator<Row> iterator = tableEnvironment.executeSql(queryTableSql).collect()) {
        while (iterator.hasNext()) {
          Row row = iterator.next();
          Assert.assertEquals(Row.of(1, 1), row);
        }
      }
    }
  }

  @Test
  public void testLookupTableSource() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = RandomUtils.randomString();
    String createTableSql1 =
        String.format(
            "CREATE TABLE `%s`.`%s` (c1 int, c2 varchar(255), PRIMARY KEY(`c1`))",
            DATABASE_NAME, tableName);
    String insertDataSql =
        String.format(
            "INSERT INTO `%s`.`%s` VALUES (1,'data1'),(2,'data2'),(3,'data3'),(4,'data4')",
            DATABASE_NAME, tableName);
    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
    String sql =
        String.format(
            "SELECT * FROM `datagen` "
                + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
                + "ON datagen.c1 = dim_table.c1 ",
            "tidb", DATABASE_NAME, tableName);

    try (CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        Object c1 = row.getField(0);
        String c2 = String.format("data%s", c1);
        boolean isJoin = (int) c1 <= 4;
        Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null, isJoin ? c2 : null);
        Assert.assertEquals(row, row1);
      }
    }
  }

  @Test
  public void testReadAtLeastOnce() throws Exception {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
    String tableName = RandomUtils.randomString();
    String values =
        IntStream.range(0, 2000).mapToObj(i -> "(" + i + ")").collect(Collectors.joining(","));
    clientSession.sqlUpdate(
        "USE " + DATABASE_NAME,
        String.format("CREATE TABLE `%s` (`c1` int unique key)", tableName),
        String.format("SPLIT TABLE `%s` BETWEEN (0) AND (2000) REGIONS %s", tableName, 2),
        String.format("INSERT INTO `%s` VALUES %s", tableName, values));
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100L);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);
    properties.put("type", "tidb");
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME, tableName);
    Table table = tableEnvironment.sqlQuery(queryTableSql);
    SingleOutputStreamOperator<RowData> dataStream =
        tableEnvironment
            .toAppendStream(table, RowData.class)
            .map(
                row -> {
                  if (RandomUtils.randomInt(1000) == 0) {
                    throw new IllegalStateException("Random test exception");
                  }
                  Thread.sleep(10L);
                  return row;
                });
    tableEnvironment.createTemporaryView("test_view", dataStream);
    CloseableIterator<Row> iterator =
        tableEnvironment.executeSql("SELECT * FROM `test_view`").collect();
    Set<Integer> results = new HashSet<>();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      results.add(((RowData) row.getFieldAs(0)).getInt(0));
    }
    Assert.assertEquals(2000, results.size());
    Assert.assertEquals(IntStream.range(0, 2000).boxed().collect(Collectors.toSet()), results);
  }

  @Test
  public void testReadExactlyOnce() throws Exception {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
    String tableName = RandomUtils.randomString();
    String values =
        IntStream.range(0, 2000).mapToObj(i -> "(" + i + ")").collect(Collectors.joining(","));
    clientSession.sqlUpdate(
        "USE " + DATABASE_NAME,
        String.format("CREATE TABLE `%s` (`c1` int unique key)", tableName),
        String.format("SPLIT TABLE `%s` BETWEEN (0) AND (2000) REGIONS %s", tableName, 2),
        String.format("INSERT INTO `%s` VALUES %s", tableName, values));
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100L);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);
    properties.put("type", "tidb");
    properties.put(TiDBOptions.SOURCE_FAILOVER.key(), "offset");
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME, tableName);
    Table table = tableEnvironment.sqlQuery(queryTableSql);
    SingleOutputStreamOperator<RowData> dataStream =
        tableEnvironment
            .toAppendStream(table, RowData.class)
            .map(
                row -> {
                  if (RandomUtils.randomInt(1000) == 0) {
                    throw new IllegalStateException("Random test exception");
                  }
                  Thread.sleep(10L);
                  return row;
                });
    tableEnvironment.createTemporaryView("test_view", dataStream);
    CloseableIterator<Row> iterator =
        tableEnvironment.executeSql("SELECT * FROM `test_view`").collect();
    List<Integer> results = new ArrayList<>();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      results.add(((RowData) row.getFieldAs(0)).getInt(0));
    }
    results.sort(Comparator.naturalOrder());
    Assert.assertEquals(2000, results.size());
    Assert.assertEquals(IntStream.range(0, 2000).boxed().collect(Collectors.toList()), results);
  }

  @Test
  @Ignore("Need kafka")
  public void testCdc() {
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    String createCatalogSql =
        "CREATE CATALOG `tidb` \n"
            + "WITH (\n"
            + "  'type' = 'tidb',\n"
            + "  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',\n"
            + "  'tidb.username' = 'root',\n"
            + "  'tidb.password' = '',\n"
            + "  'tidb.metadata.included' = '*',\n"
            + "  'tidb.streaming.source' = 'kafka',\n"
            + "  'tidb.streaming.codec' = 'json',\n"
            + "  'tidb.streaming.kafka.bootstrap.servers' = 'localhost:9092',\n"
            + "  'tidb.streaming.kafka.topic' = 'test_cdc',\n"
            + "  'tidb.streaming.kafka.group.id' = 'test_cdc_group',\n"
            + "  'tidb.streaming.ignore-parse-errors' = 'true'\n"
            + ")";
    tableEnvironment.executeSql(createCatalogSql);
    tableEnvironment.executeSql("SELECT * FROM `tidb`.`test`.`test_cdc`").print();
  }
}
