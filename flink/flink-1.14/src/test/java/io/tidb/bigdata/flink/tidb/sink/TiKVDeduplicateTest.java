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
import static io.tidb.bigdata.flink.connector.TiDBOptions.DELETE_ENABLE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_PARSE_ERRORS;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_BUFFER_SIZE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_CODEC;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_CODEC_JSON;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TiKVDeduplicateTest extends FlinkTestBase {

  private String srcTable;

  private String dstTable;

  private static final String TABLE_UK =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint(20) NULL DEFAULT NULL,\n"
          + "    c2  bigint(20) NOT NULL,\n"
          + "    UNIQUE INDEX `uniq_1`(`c1`) USING BTREE,\n"
          + "    UNIQUE INDEX `uniq_2`(`c2`) USING BTREE\n"
          + ")";

  @Test
  public void testDeduplicate() throws Exception {
    srcTable = "flink_deduplicate_src_test" + RandomUtils.randomString();
    dstTable = "flink_deduplicate_dst_test" + RandomUtils.randomString();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    Map<String, String> properties = defaultProperties();
    // source
    properties.put(STREAMING_SOURCE.key(), "kafka");
    properties.put(STREAMING_CODEC.key(), STREAMING_CODEC_JSON);
    properties.put("tidb.streaming.kafka.bootstrap.servers", "localhost:9092");
    properties.put("tidb.streaming.kafka.topic", "tidb_test");
    properties.put("tidb.streaming.kafka.group.id", "group_dep");
    properties.put(IGNORE_PARSE_ERRORS.key(), "true");
    // sink
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_BUFFER_SIZE.key(), "2");
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(DEDUPLICATE.key(), Boolean.toString(true));
    properties.put(DELETE_ENABLE.key(), Boolean.toString(true));

    executor.execute(
        () -> {
          EnvironmentSettings settings =
              EnvironmentSettings.newInstance().inStreamingMode().build();
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
          // init catalog and create dstTable
          TiDBCatalog tiDBCatalog =
              initTiDBCatalog(dstTable, TABLE_UK, tableEnvironment, properties);
          // create src table
          tiDBCatalog.sqlUpdate(
              String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
          tiDBCatalog.sqlUpdate(String.format(TABLE_UK, DATABASE_NAME, srcTable));

          tableEnvironment.sqlUpdate(
              String.format(
                  "INSERT INTO `tidb`.`%s`.`%s` SELECT c1,c2 FROM `tidb`.`%s`.`%s`",
                  DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
          try {
            tableEnvironment.execute("test");
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

    try {
      // wait stream job ready
      Thread.sleep(20000);

      // insert (1,1)
      testDatabase
          .getClientSession()
          .sqlUpdate(
              String.format("insert into `%s`.`%s` values('1','1')", DATABASE_NAME, srcTable));
      // delete (1,1)
      testDatabase
          .getClientSession()
          .sqlUpdate(
              String.format("delete from `%s`.`%s` where c1 = '1'", DATABASE_NAME, srcTable));
      // trigger row flush
      testDatabase
          .getClientSession()
          .sqlUpdate(
              String.format(
                  "insert into `%s`.`%s` values('3','3'),('4','4')", DATABASE_NAME, srcTable));
      Thread.sleep(20000);

      // +I(3,3) will do 2pc
      Assert.assertEquals(
          1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, dstTable));

    } finally {
      executor.shutdownNow();
    }
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
