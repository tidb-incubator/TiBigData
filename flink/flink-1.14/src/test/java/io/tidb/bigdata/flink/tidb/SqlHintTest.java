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

import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.CHECKPOINT;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class SqlHintTest extends FlinkTestBase {

  private String srcTable;

  private String dstTable;

  @Test
  public void testSqlHint() throws Exception {
    final int rowCount = 100000;
    srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    dstTable = RandomUtils.randomString();

    TableEnvironment tableEnvironment = getBatchTableEnvironment();

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), CHECKPOINT.name());

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, TABLE_WITH_INDEX, tableEnvironment, properties);

    String sql =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tikv.sink.transaction'='global') */"
                + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
                + "FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");

    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
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
