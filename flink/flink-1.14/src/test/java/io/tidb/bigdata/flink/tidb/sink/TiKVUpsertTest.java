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

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVUpsertTest extends FlinkTestBase {

  private static final String TABLE =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "("
          + "  `id` bigint(20) NOT NULL,\n"
          + "  `name` varchar(255) NULL DEFAULT NULL,\n"
          + "  PRIMARY KEY (`id`) USING BTREE\n"
          + ")";

  private final SinkTransaction transaction;
  private final Supplier<TableEnvironment> tableEnvironmentSupplier;
  private final String mode;

  public TiKVUpsertTest(
      SinkTransaction transaction,
      Supplier<TableEnvironment> tableEnvironmentSupplier,
      String mode) {
    this.transaction = transaction;
    this.tableEnvironmentSupplier = tableEnvironmentSupplier;
    this.mode = mode;
  }

  @Parameters(name = "{index}: mode={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            SinkTransaction.MINIBATCH,
            (Supplier<TableEnvironment>) FlinkTestBase::getBatchTableEnvironment,
            "Batch"
          },
          {
            SinkTransaction.GLOBAL,
            (Supplier<TableEnvironment>) FlinkTestBase::getBatchTableEnvironment,
            "Batch"
          },
          {
            SinkTransaction.MINIBATCH,
            (Supplier<TableEnvironment>) FlinkTestBase::getStreamingTableEnvironment,
            "Streaming"
          },
        });
  }

  @Test
  public void testUpsert() throws Exception {
    String dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");

    initTiDBCatalog(dstTable, TABLE, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "VALUES(1, 'before'), (2, 'before')",
            DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "VALUES(1, 'after'), (2, 'after')",
            DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, after]", "+I[2, after]"), dstTable);
  }
}
