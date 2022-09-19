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

package io.tidb.bigdata.flink.tidb.rowid;

import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_AUTOINCREMENT_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_AUTO_RANDOM_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator.RowIDAllocatorType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(org.junit.runners.Parameterized.class)
@Category(IntegrationTest.class)
public class AutoGenerateRowIdTest extends FlinkTestBase {

  @Parameters(name = "{index}: RowIDAllocatorType={1}, isUnsigned={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][]{
            {
                "CREATE TABLE `%s`.`%s`(id bigint, number bigint) shard_row_id_bits=15",
                RowIDAllocatorType.IMPLICIT_ROWID,
                false,
                15
            },
            {
                "CREATE TABLE `%s`.`%s`(id bigint signed PRIMARY KEY AUTO_INCREMENT, number bigint)",
                RowIDAllocatorType.AUTO_INCREMENT,
                false,
                0
            },
            {
                "CREATE TABLE `%s`.`%s`(id bigint signed PRIMARY KEY AUTO_INCREMENT, number bigint)",
                RowIDAllocatorType.AUTO_INCREMENT,
                false,
                0
            },
            {
                "CREATE TABLE `%s`.`%s`(id bigint unsigned PRIMARY KEY AUTO_RANDOM(15), number bigint)",
                RowIDAllocatorType.AUTO_RANDOM,
                true,
                15
            },
            {
                "CREATE TABLE `%s`.`%s`(id bigint PRIMARY KEY AUTO_RANDOM(15), number bigint)",
                RowIDAllocatorType.AUTO_RANDOM,
                false,
                15
            }
        });
  }

  private final String createTableSql;
  private final RowIDAllocatorType type;
  private final boolean isUnsigned;
  private final int shardBits;

  public AutoGenerateRowIdTest(String createTableSql, RowIDAllocatorType type, boolean isUnsigned,
      int sharedBits) {
    this.createTableSql = createTableSql;
    this.type = type;
    this.isUnsigned = isUnsigned;
    this.shardBits = sharedBits;
  }

  private String srcTable;

  private String dstTable;

  @Test
  public void testAutoGenerateRowIdGlobal() throws Exception {
    final int rowCount = 100;
    srcTable = RandomUtils.randomString();
    generateAutoRandomData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.GLOBAL.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "true");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "SELECT c1, c2 " + "FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testAutoGenerateRowIdBatchMiniBatch() throws Exception {
    final int rowCount = 3000;
    srcTable = RandomUtils.randomString();
    generateAutoRandomData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "true");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "SELECT c1, c2 " + "FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testAutoGenerateRowIdStreamingMiniBatch() throws Exception {
    final int rowCount = 3000;
    srcTable = RandomUtils.randomString();
    generateAutoRandomData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100L);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(1, TimeUnit.SECONDS)));
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "true");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "SELECT c1, c2 " + "FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testTiBigDataOverflow() throws Exception {
    final int rowCount = 3;
    srcTable = RandomUtils.randomString();
    generateAutoRandomData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "true");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);
    int signBitLength = isUnsigned ? 0 : 1;
    long size = (long) (Math.pow(2, 64 - shardBits - signBitLength) - 3);
    tiDBCatalog.createRowIDAllocator(DATABASE_NAME, dstTable, size, type);
    try {
      tableEnvironment.sqlUpdate(
          String.format(
              "INSERT INTO `tidb`.`%s`.`%s` " + "SELECT c1, c2 " + "FROM `tidb`.`%s`.`%s`",
              DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
      tableEnvironment.execute("test");
      fail("Expected an TiBatchWriteException or AllocateRowIDOverflowException to be thrown");
    } catch (JobExecutionException e) {
      switch (type) {
        case IMPLICIT_ROWID:
        case AUTO_RANDOM:
          assertEquals(e.getCause().getCause().getClass().getName(),
              "org.tikv.common.exception.AllocateRowIDOverflowException");
          break;
        case AUTO_INCREMENT:
          assertEquals(e.getCause().getCause().getCause().getClass().getName(),
              "org.tikv.common.exception.TiBatchWriteException");
          break;
        default:
          fail("Unexpected RowIDAllocator type");
      }
    }
  }

  @Test
  public void testTiDBOverflow() throws Exception {
    final int rowCount = 3;
    srcTable = RandomUtils.randomString();
    generateAutoRandomData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(1, TimeUnit.SECONDS)));
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "true");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);
    int signBitLength = isUnsigned ? 0 : 1;
    long size = (long) (Math.pow(2, 64 - this.shardBits - signBitLength) - 3);
    tiDBCatalog.createRowIDAllocator(DATABASE_NAME, dstTable, size, type);
    tableEnvironment.executeSql(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "SELECT c1, c2 " + "FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
    assertNotEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  private static void generateAutoRandomData(String tableName, int rowCount) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
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
                + "    c2  bigint\n"
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
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, tableName));
  }

  @After
  public void teardown() {
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, dstTable));
  }
}
