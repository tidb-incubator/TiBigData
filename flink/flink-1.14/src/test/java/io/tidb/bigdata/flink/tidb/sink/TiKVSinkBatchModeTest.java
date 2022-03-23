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

package io.tidb.bigdata.flink.tidb.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DEDUPLICATE;
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
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVSinkBatchModeTest extends FlinkTestBase {

  @Parameters(name = "{index}: Transaction={0}, WriteMode={1}, Deduplicate={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{SinkTransaction.GLOBAL, TiDBWriteMode.APPEND, false},
        {SinkTransaction.GLOBAL, TiDBWriteMode.UPSERT, true},
        {SinkTransaction.MINIBATCH, TiDBWriteMode.APPEND, false},
        {SinkTransaction.MINIBATCH, TiDBWriteMode.UPSERT, true}});
  }

  private final SinkTransaction transaction;
  private final TiDBWriteMode writeMode;
  private final boolean deduplicate;

  public TiKVSinkBatchModeTest(SinkTransaction transaction, TiDBWriteMode writeMode,
      boolean deduplicate) {
    this.transaction = transaction;
    this.writeMode = writeMode;
    this.deduplicate = deduplicate;
  }

  @Test
  public void testMiniBatchTransaction() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    StreamTableEnvironment tableEnvironment = getBatchModeStreamTableEnvironment();

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DEDUPLICATE.key(), Boolean.toString(deduplicate));
    properties.put(WRITE_MODE.key(), writeMode.name());

    TiDBCatalog tiDBCatalog = initTiDBCatalog(dstTable,
        deduplicate ? TABLE_WITH_INDEX : TABLE_WITHOUT_INDEX, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(String.format("INSERT INTO `tidb`.`test`.`%s` "
        + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
        + "FROM `tidb`.`test`.`%s`", dstTable, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }
}