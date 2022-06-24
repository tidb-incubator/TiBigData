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

package io.tidb.bigdata.flink.tidb.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DEDUPLICATE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVDeduplicateNullableIndexTest extends FlinkTestBase {

  @Parameters(name = "{index}: Transaction={0}, WriteMode={1}, Deduplicate={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {SinkTransaction.GLOBAL, TiDBWriteMode.APPEND, false},
          {SinkTransaction.GLOBAL, TiDBWriteMode.UPSERT, true},
          {SinkTransaction.MINIBATCH, TiDBWriteMode.APPEND, false},
          {SinkTransaction.MINIBATCH, TiDBWriteMode.UPSERT, true}
        });
  }

  private final SinkTransaction transaction;
  private final TiDBWriteMode writeMode;
  private final boolean deduplicate;

  public TiKVDeduplicateNullableIndexTest(
      SinkTransaction transaction, TiDBWriteMode writeMode, boolean deduplicate) {
    this.transaction = transaction;
    this.writeMode = writeMode;
    this.deduplicate = deduplicate;
  }

  private String dstTable;

  private static final String TABLE_UK_NULLABLE =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "("
          + "  `id`   bigint(20),\n"
          + "  `age`  int(11) NULL DEFAULT NULL,\n"
          + "  `name` varchar(10),\n"
          + "  unique key(`id`),"
          + "  unique key(`age`)"
          + ")";

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testDeduplicateWithNullUniqueIndex() throws Exception {
    dstTable = "flink_deduplicate_dst_test" + RandomUtils.randomString();

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    TableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    String dstTable = RandomUtils.randomString();
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DEDUPLICATE.key(), Boolean.toString(deduplicate));
    properties.put(WRITE_MODE.key(), writeMode.name());

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, TABLE_UK_NULLABLE, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` "
                + "VALUES(1, cast(null as int), 'tony'), (2, cast(null as int), 'mike')",
            DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");

    checkRowResult(
        tableEnvironment, Lists.newArrayList("+I[1, null, tony]", "+I[2, null, mike]"), dstTable);
  }

  @Test
  public void testDeduplicateWithNullUniqueIndexAndConflictIndex() throws Exception {
    // If deduplicate is off, we expect a IllegalStateException.
    if (!deduplicate) {
      exceptionRule.expectCause(
          hasProperty(
              "cause",
              allOf(
                  isA(IllegalStateException.class),
                  hasProperty(
                      "message",
                      containsString("Duplicate index in one batch, please enable deduplicate")))));
    }

    dstTable = "flink_deduplicate_dst_test" + RandomUtils.randomString();

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    TableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    String dstTable = RandomUtils.randomString();
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DEDUPLICATE.key(), Boolean.toString(deduplicate));
    properties.put(WRITE_MODE.key(), writeMode.name());

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, TABLE_UK_NULLABLE, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` "
                + "VALUES(1, cast(null as int), 'tony'), (1, cast(null as int), 'mike')",
            DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");

    // If deduplicate is on, only one row will be sinked.
    if (deduplicate) {
      Assert.assertEquals(
          1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, dstTable));
    }
  }
}
