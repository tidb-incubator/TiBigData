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

package io.tidb.bigdata.flink.tidb.delete;

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
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.test.StreamIntegrationTest;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/** append mode will throw exception Global is not tested for it can't work with streaming */
@Category(StreamIntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVDeleteNotSupportTest extends FlinkTestBase {

  private String srcTable;

  private String dstTable;

  public TiKVDeleteNotSupportTest(
      SinkTransaction transaction,
      TiDBWriteMode writeMode,
      boolean enableDelete,
      String flinkDeleteTable,
      String kafkaGroup) {
    this.transaction = transaction;
    this.writeMode = writeMode;
    this.enableDelete = enableDelete;
    this.flinkDeleteTable = flinkDeleteTable;
    this.kafkaGroup = kafkaGroup;
  }

  @Parameters(
      name =
          "{index}: Transaction={0}, WriteMode={1}, EnableDelete={2},FlinkDeleteTable={3} ,KafkaGroup={4} ")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            SinkTransaction.MINIBATCH,
            TiDBWriteMode.APPEND,
            true,
            TABLE_CLUSTER_BIGINT,
            "group_d_u_1"
          },
        });
  }

  private final SinkTransaction transaction;
  private final TiDBWriteMode writeMode;
  private final boolean enableDelete;
  private final String flinkDeleteTable;
  private final String kafkaGroup;

  private static final String TABLE_CLUSTER_BIGINT =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint(20),\n"
          + "    c2  varchar(255),\n"
          + "    PRIMARY KEY (`c1`) /*T![clustered_index] CLUSTERED */\n"
          + ")";

  @Test(expected = Exception.class)
  public void testDeleteNotSupport() throws Exception {
    srcTable = "flink_delete_src_test" + RandomUtils.randomString();
    dstTable = "flink_delete_dst_test" + RandomUtils.randomString();

    Map<String, String> properties = defaultProperties();
    // source
    properties.put(STREAMING_SOURCE.key(), "kafka");
    properties.put(STREAMING_CODEC.key(), STREAMING_CODEC_JSON);
    properties.put("tidb.streaming.kafka.bootstrap.servers", "localhost:9092");
    properties.put("tidb.streaming.kafka.topic", "tidb_test");
    properties.put("tidb.streaming.kafka.group.id", kafkaGroup);
    properties.put(IGNORE_PARSE_ERRORS.key(), "true");
    // sink
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_BUFFER_SIZE.key(), "1");
    properties.put(WRITE_MODE.key(), writeMode.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DELETE_ENABLE.key(), Boolean.toString(enableDelete));

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // init catalog and create dstTable
    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, flinkDeleteTable, tableEnvironment, properties);
    // create src table
    tiDBCatalog.sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
    tiDBCatalog.sqlUpdate(String.format(flinkDeleteTable, DATABASE_NAME, srcTable));

    tableEnvironment.executeSql(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` SELECT c1,c2 FROM `tidb`.`%s`.`%s`",
            DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));
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
