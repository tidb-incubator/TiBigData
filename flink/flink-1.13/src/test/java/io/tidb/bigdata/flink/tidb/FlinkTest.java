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

import static io.tidb.bigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.catalog.TiDBCatalog;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.test.TableUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class FlinkTest {

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "test";

  public static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS `test`";

  public static final String CREATE_TABLE_SQL_FORMAT =
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
          + ")";

  public static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE IF EXISTS `%s`.`%s`";

  // for write mode, only unique key and primary key is mutable.
  public static final String INSERT_ROW_SQL_FORMAT =
      "INSERT INTO `%s`.`%s`.`%s`\n"
          + "VALUES (\n"
          + " cast(%s as tinyint) ,\n"
          + " cast(%s as smallint) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as bigint) ,\n"
          + " cast('chartype' as char(10)),\n"
          + " cast('varchartype' as varchar(20)),\n"
          + " cast('tinytexttype' as string),\n"
          + " cast('mediumtexttype' as string),\n"
          + " cast('texttype' as string),\n"
          + " cast('longtexttype' as string),\n"
          + " cast('binarytype' as bytes),\n"
          + " cast('varbinarytype' as bytes),\n"
          + " cast('tinyblobtype' as bytes),\n"
          + " cast('mediumblobtype' as bytes),\n"
          + " cast('blobtype' as bytes),\n"
          + " cast('longblobtype' as bytes),\n"
          + " cast(1.234 as float),\n"
          + " cast(2.456789 as double),\n"
          + " cast(123.456 as decimal(6,3)),\n"
          + " cast('2020-08-10' as date),\n"
          + " cast('15:30:29' as time),\n"
          + " cast('2020-08-10 15:30:29' as timestamp),\n"
          + " cast('2020-08-10 16:30:29' as timestamp),\n"
          + " cast(2020 as smallint),\n"
          + " cast(true as tinyint),\n"
          + " cast('{\"a\":1,\"b\":2}' as string),\n"
          + " cast('1' as string),\n"
          + " cast('a' as string)\n"
          + ")";

  public static final String CREATE_DATAGEN_TABLE_SQL = "CREATE TABLE datagen (\n"
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

  public static String getInsertRowSql(String tableName, byte value1, short value2) {
    return format(INSERT_ROW_SQL_FORMAT, CATALOG_NAME, DATABASE_NAME, tableName, value1, value2);
  }

  public static String getCreateTableSql(String tableName) {
    return String.format(CREATE_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public static String getDropTableSql(String tableName) {
    return String.format(DROP_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inBatchMode().build();
    return TableEnvironment.create(settings);
  }

  public Row runByCatalog(Map<String, String> properties) throws Exception {
    return runByCatalog(properties, null, null);
  }

  public Row runByCatalog(Map<String, String> properties, String resultSql, String tableName)
      throws Exception {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    if (tableName == null) {
      tableName = RandomUtils.randomString();
    }
    String dropTableSql = getDropTableSql(tableName);
    String createTableSql = getCreateTableSql(tableName);
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, dropTableSql, createTableSql);
    // register catalog
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 1));
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 2));
    // query
    if (resultSql == null) {
      resultSql = format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, tableName);
    }
    TableResult tableResult = tableEnvironment.executeSql(resultSql);
    Row row = tableResult.collect().next();
    tiDBCatalog.sqlUpdate(dropTableSql);
    return row;
  }

  public Row copyRow(Row row) {
    Row newRow = new Row(row.getArity());
    for (int i = 0; i < row.getArity(); i++) {
      newRow.setField(i, row.getField(i));
    }
    return newRow;
  }

  public Row copyRow(Row row, int[] indexes) {
    Row newRow = new Row(indexes.length);
    for (int i = 0; i < indexes.length; i++) {
      newRow.setField(i, row.getField(indexes[i]));
    }
    return newRow;
  }

  public Row replicaRead() throws Exception {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put(TIDB_REPLICA_READ, "follower,leader");
    return runByCatalog(properties);
  }

  public Row upsertAndRead() throws Exception {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put(TIDB_WRITE_MODE, "upsert");
    return runByCatalog(properties);
  }

  @Test
  public void testCatalog() throws Exception {
    // read by limit
    String tableName = RandomUtils.randomString();
    Row row = runByCatalog(ConfigUtils.defaultProperties(),
        format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1", CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    // replica read
    Assert.assertEquals(row, replicaRead());
    // upsert and read
    Row row1 = copyRow(row);
    row1.setField(0, (byte) 1);
    row1.setField(1, (short) 2);
    Assert.assertEquals(row1, upsertAndRead());
    // filter push down
    tableName = RandomUtils.randomString();
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put(TIDB_FILTER_PUSH_DOWN, "true");
    Assert.assertEquals(row,
        runByCatalog(properties,
            format("SELECT * FROM `%s`.`%s`.`%s` WHERE (c1 = 1 OR c3 = 1) AND c2 = 1",
                CATALOG_NAME, DATABASE_NAME, tableName),
            tableName));
    // column pruner
    tableName = RandomUtils.randomString();
    // select 10 column randomly
    Random random = new Random();
    int[] ints = IntStream.range(0, 10).map(i -> random.nextInt(29)).toArray();
    row1 = runByCatalog(ConfigUtils.defaultProperties(),
        format("SELECT %s FROM `%s`.`%s`.`%s` LIMIT 1",
            Arrays.stream(ints).mapToObj(i -> "c" + (i + 1)).collect(Collectors.joining(",")),
            CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    Assert.assertEquals(row1, copyRow(row, ints));
  }

  @Test
  public void testLookupTableSource() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = ConfigUtils.defaultProperties();
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = RandomUtils.randomString();
    String createTableSql1 = String
        .format("CREATE TABLE `%s`.`%s` (c1 int, c2 varchar(255), PRIMARY KEY(`c1`))",
            DATABASE_NAME, tableName);
    String insertDataSql = String
        .format("INSERT INTO `%s`.`%s` VALUES (1,'data1'),(2,'data2'),(3,'data3'),(4,'data4')",
            DATABASE_NAME, tableName);
    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
    String sql = String.format(
        "SELECT * FROM `datagen` "
            + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
            + "ON datagen.c1 = dim_table.c1 ",
        "tidb", DATABASE_NAME, tableName);
    CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Object c1 = row.getField(0);
      String c2 = String.format("data%s", c1);
      boolean isJoin = (int) c1 <= 4;
      Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null, isJoin ? c2 : null);
      Assert.assertEquals(row, row1);
    }
  }

  @Test
  public void testSnapshotRead() throws Exception {
    for (int i = 1; i <= 3; i++) {
      // insert
      Map<String, String> properties = ConfigUtils.defaultProperties();
      ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
      String tableName = RandomUtils.randomString();
      clientSession.sqlUpdate(String.format("CREATE TABLE `%s` (`c1` int,`c2` int)", tableName),
          String.format("INSERT INTO `%s` VALUES(1,1)", tableName));

      if (i == 1) {
        // get timestamp
        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
            ZonedDateTime.now().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        // wait for 1 second, because we use client time rather than server time
        Thread.sleep(1000L);
      } else {
        // get version
        long version = clientSession.getSnapshotVersion().getVersion();
        properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
      }

      // update
      clientSession.sqlUpdate(String.format("UPDATE `%s` SET c1 = 2 WHERE c1 =1", tableName));

      if (i == 3) {
        // get timestamp
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
            zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
      }

      // read by version
      TableEnvironment tableEnvironment = getTableEnvironment();
      properties.put("type", "tidb");
      String createCatalogSql = format("CREATE CATALOG `tidb` WITH ( %s )",
          TableUtils.toSqlProperties(properties));
      tableEnvironment.executeSql(createCatalogSql);
      String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME,
          tableName);
      CloseableIterator<Row> iterator = tableEnvironment.executeSql(queryTableSql).collect();
      while (iterator.hasNext()) {
        Row row = iterator.next();
        Assert.assertEquals(Row.of(1, 1), row);
      }
      iterator.close();
    }
  }


  @Test
  public void testReadAtLeastOnce() throws Exception {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
    String tableName = RandomUtils.randomString();
    String values = IntStream.range(0, 2000).mapToObj(i -> "(" + i + ")")
        .collect(Collectors.joining(","));
    clientSession.sqlUpdate(
        String.format("CREATE TABLE `%s` (`c1` int unique key)", tableName),
        String.format("SPLIT TABLE `%s` BETWEEN (0) AND (2000) REGIONS %s", tableName, 2),
        String.format("INSERT INTO `%s` VALUES %s", tableName, values));
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100L);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);
    properties.put("type", "tidb");
    String createCatalogSql = format("CREATE CATALOG `tidb` WITH ( %s )",
        TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME,
        tableName);
    Table table = tableEnvironment.sqlQuery(queryTableSql);
    SingleOutputStreamOperator<RowData> dataStream = tableEnvironment.toAppendStream(
        table, RowData.class).map(row -> {
      if (RandomUtils.randomInt(1000) == 0) {
        throw new IllegalStateException("Random test exception");
      }
      Thread.sleep(10L);
      return row;
    });
    tableEnvironment.createTemporaryView("test_view", dataStream);
    CloseableIterator<Row> iterator = tableEnvironment.executeSql("SELECT * FROM `test_view`")
        .collect();
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
    String values = IntStream.range(0, 2000).mapToObj(i -> "(" + i + ")")
        .collect(Collectors.joining(","));
    clientSession.sqlUpdate(
        String.format("CREATE TABLE `%s` (`c1` int unique key)", tableName),
        String.format("SPLIT TABLE `%s` BETWEEN (0) AND (2000) REGIONS %s", tableName, 2),
        String.format("INSERT INTO `%s` VALUES %s", tableName, values));
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100L);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    env.setParallelism(1);
    properties.put("type", "tidb");
    properties.put(TiDBOptions.SOURCE_FAILOVER.key(), "offset");
    String createCatalogSql = format("CREATE CATALOG `tidb` WITH ( %s )",
        TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME,
        tableName);
    Table table = tableEnvironment.sqlQuery(queryTableSql);
    SingleOutputStreamOperator<RowData> dataStream = tableEnvironment.toAppendStream(
        table, RowData.class).map(row -> {
      if (RandomUtils.randomInt(1000) == 0) {
        throw new IllegalStateException("Random test exception");
      }
      Thread.sleep(10L);
      return row;
    });
    tableEnvironment.createTemporaryView("test_view", dataStream);
    CloseableIterator<Row> iterator = tableEnvironment.executeSql("SELECT * FROM `test_view`")
        .collect();
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
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    String createCatalogSql = "CREATE CATALOG `tidb` \n"
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
