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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/** Test for pk, uk and multiple_uk table */
@Category(StreamIntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVDeleteTest extends FlinkTestBase {

  private String srcTable;

  private String dstTable;

  public TiKVDeleteTest(String flinkDeleteTable, int result, String kafkaGroup) {
    this.flinkDeleteTable = flinkDeleteTable;
    this.result = result;
    this.kafkaGroup = kafkaGroup;
  }

  @Parameters(name = "{index}: FlinkDeleteTable={0}, Result={1} ,KafkaGroup={2} ")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {TABLE_PK, 2, "group_d_1"}, {TABLE_UK, 2, "group_d_2"}, {TABLE_MUTILE_UK, 2, "group_d_3"},
        });
  }

  private final String flinkDeleteTable;
  private final int result;
  private final String kafkaGroup;

  private static final String TABLE_PK =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  varchar(255),\n"
          + "    c2  varchar(255),\n"
          + "    PRIMARY KEY (`c1`) \n"
          + ")";

  private static final String TABLE_UK =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint(20) NULL DEFAULT NULL,\n"
          + "    c2  bigint(20) NOT NULL,\n"
          + "    UNIQUE INDEX `uniq_1`(`c1`) USING BTREE,\n"
          + "    UNIQUE INDEX `uniq_2`(`c2`) USING BTREE\n"
          + ")";

  private static final String TABLE_MUTILE_UK =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint(20) NOT NULL,\n"
          + "    c2  bigint(20) NOT NULL,\n"
          + "    UNIQUE INDEX `uniq_1`(`c1`,`c2`) USING BTREE\n"
          + ")";

  @Test
  public void testDelete() throws Exception {
    srcTable = "flink_delete_src_test" + RandomUtils.randomString();
    dstTable = "flink_delete_dst_test" + RandomUtils.randomString();

    ExecutorService executor = Executors.newSingleThreadExecutor();

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
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(DELETE_ENABLE.key(), Boolean.toString(true));

    FutureTask<Optional<JobClient>> task =
        new FutureTask<>(
            () -> {
              EnvironmentSettings settings =
                  EnvironmentSettings.newInstance().inStreamingMode().build();
              StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
              StreamTableEnvironment tableEnvironment =
                  StreamTableEnvironment.create(env, settings);
              // init catalog and create dstTable
              TiDBCatalog tiDBCatalog =
                  initTiDBCatalog(dstTable, flinkDeleteTable, tableEnvironment, properties);
              // create src table
              tiDBCatalog.sqlUpdate(
                  String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
              tiDBCatalog.sqlUpdate(String.format(flinkDeleteTable, DATABASE_NAME, srcTable));

              TableResult tableResult =
                  tableEnvironment.executeSql(
                      String.format(
                          "INSERT INTO `tidb`.`%s`.`%s` SELECT c1,c2 FROM `tidb`.`%s`.`%s`",
                          DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));

              return tableResult.getJobClient();
            });
    executor.submit(task);
    // wait stream job ready
    Thread.sleep(20000);

    JobClient jobClient = task.get().orElseThrow(IllegalStateException::new);

    // insert 4 rows in src, flush 3 insert to dst
    testDatabase
        .getClientSession()
        .sqlUpdate(
            String.format(
                "insert into `%s`.`%s` values('1','1'),('2','2'),('3','3'),('4','4')",
                DATABASE_NAME, srcTable));
    Thread.sleep(20000);
    Assert.assertEquals(
        4, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, srcTable));
    Assert.assertEquals(
        3, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, dstTable));
    // delete 3 rows in src , flush 1 insert and 2 delete to dst
    testDatabase
        .getClientSession()
        .sqlUpdate(
            String.format(
                "delete from `%s`.`%s` where c1 = '1' or c1 = '2' or c1 = '3'",
                DATABASE_NAME, srcTable));
    Thread.sleep(20000);
    Assert.assertEquals(
        1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, srcTable));
    Assert.assertEquals(
        result, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, dstTable));

    jobClient.cancel();
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
