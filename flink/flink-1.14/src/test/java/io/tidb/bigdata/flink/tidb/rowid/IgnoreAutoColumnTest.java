/*
 * Copyright 2022 TiDB Project Authors.
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
 */

package io.tidb.bigdata.flink.tidb.rowid;

import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_AUTOINCREMENT_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_AUTO_RANDOM_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator.RowIDAllocatorType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(org.junit.runners.Parameterized.class)
@Category(IntegrationTest.class)
public class IgnoreAutoColumnTest extends FlinkTestBase {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Parameters(name = "{index}: RowIDAllocatorType={1}, isUnsigned={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            "CREATE TABLE `%s`.`%s`(id bigint signed PRIMARY KEY AUTO_INCREMENT, number bigint)",
            RowIDAllocatorType.AUTO_INCREMENT,
            false
          },
          {
            "CREATE TABLE `%s`.`%s`(id bigint signed PRIMARY KEY AUTO_INCREMENT, number bigint)",
            RowIDAllocatorType.AUTO_INCREMENT,
            false
          },
          {
            "CREATE TABLE `%s`.`%s`(id bigint unsigned PRIMARY KEY AUTO_RANDOM, number bigint)",
            RowIDAllocatorType.AUTO_RANDOM,
            false
          },
          {
            "CREATE TABLE `%s`.`%s`(id bigint unsigned PRIMARY KEY AUTO_RANDOM, number bigint)",
            RowIDAllocatorType.AUTO_RANDOM,
            false
          }
        });
  }

  private final String createTableSql;
  private final RowIDAllocatorType type;
  private final boolean isUnsigned;

  public IgnoreAutoColumnTest(String createTableSql, RowIDAllocatorType type, boolean isUnsigned) {
    this.createTableSql = createTableSql;
    this.type = type;
    this.isUnsigned = isUnsigned;
  }

  private String dstTable;

  @Test
  public void testIgnoreAutoColumn() throws Exception {
    dstTable = RandomUtils.randomString();
    StreamTableEnvironment tableEnvironment = getBatchModeStreamTableEnvironment();

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
            "INSERT INTO `tidb`.`%s`.`%s` (id, number) VALUES (10, 1)", DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    Row next = table.execute().collect().next();
    Assert.assertNotEquals(10L, ((Number) next.getField(0)).longValue());
  }

  @Test
  public void testNotIgnoreAutoColumn() throws Exception {
    dstTable = RandomUtils.randomString();
    StreamTableEnvironment tableEnvironment = getBatchModeStreamTableEnvironment();

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.GLOBAL.name());
    properties.put(WRITE_MODE.key(), "append");
    properties.put(IGNORE_AUTO_RANDOM_COLUMN_VALUE.key(), "false");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "false");

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` (id, number) VALUES (10, 1)", DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    Row next = table.execute().collect().next();
    Assert.assertEquals(10L, ((Number) next.getField(0)).longValue());
  }

  @After
  public void teardown() {
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, dstTable));
  }
}
