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

package io.tidb.bigdata.flink.tidb.partition;

import static io.tidb.bigdata.flink.connector.TiDBOptions.*;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(StreamIntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class PartitionDeleteTest extends FlinkTestBase {

  private static final String SRC_TABLE = "partition_delete_test_src_table";
  private static final String DST_TABLE = "partition_delete_test_dst_table";
  private final String createTableSql;
  private final String insertSql;
  private final String deleteSql;

  public PartitionDeleteTest(String createTableSql, String insertSql, String deleteSql) {
    this.createTableSql = createTableSql;
    this.insertSql = insertSql;
    this.deleteSql = deleteSql;
  }

  @Parameters(name = "{index}: CreateTableSql = {0}, InsertSql = {1}, DeleteSql = {2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {hashPartitionCreateSql, hashPartitionInsertSql, hashPartitionDeleteSql},
          {rangePartitionCreateSql, rangePartitionInsertSql, rangePartitionDeleteSql},
          {columnPartitionCreateSql, columnPartitionInsertSql, columnPartitionDeleteSql}
        });
  }

  private static final String hashPartitionCreateSql =
      "CREATE TABLE `%s`.`%s` (`id` BIGINT PRIMARY KEY, `name` VARCHAR(16)) "
          + "PARTITION BY HASH(`id`) "
          + "PARTITIONS 3";
  private static final String hashPartitionInsertSql =
      "INSERT INTO `%s`.`%s` VALUES (5, 'Apple'), (25, 'Honey'), (29, 'Mike')";
  private static final String hashPartitionDeleteSql = "DELETE FROM `%s`.`%s` WHERE `id` < 26";

  private static final String rangePartitionCreateSql =
      "CREATE TABLE `%s`.`%s` (`id` BIGINT PRIMARY KEY, `name` VARCHAR(16)) "
          + "PARTITION BY RANGE (`id`) "
          + "(PARTITION p0 VALUES LESS THAN (20), "
          + "PARTITION p1 VALUES LESS THAN (60), "
          + "PARTITION p2 VALUES LESS THAN MAXVALUE)";
  private static final String rangePartitionInsertSql =
      "INSERT INTO `%s`.`%s` VALUES (19, 'Mike'), (59, 'Apple'), (376, 'Jack')";
  private static final String rangePartitionDeleteSql = "DELETE FROM `%s`.`%s` WHERE `id` < 60";

  private static final String columnPartitionCreateSql =
      "CREATE TABLE `%s`.`%s` (`product` varbinary(16) PRIMARY KEY, price int) "
          + "PARTITION BY RANGE COLUMNS (`product`) "
          + "(PARTITION p0 VALUES LESS THAN (X'424242424242'),"
          + "PARTITION p1 VALUES LESS THAN (X'525252525252'),"
          + "PARTITION p2 VALUES LESS THAN MAXVALUE)";
  private static final String columnPartitionInsertSql =
      "INSERT INTO `%s`.`%s` VALUES ('Apple', 10), ('Orange', 20), ('Peach', 30)";
  private static final String columnPartitionDeleteSql =
      "DELETE FROM `%s`.`%s` WHERE `product` <= 'Orange'";

  @Test
  public void testDelete() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Map<String, String> properties = defaultProperties();
    // source
    properties.put(STREAMING_SOURCE.key(), "kafka");
    properties.put(STREAMING_CODEC.key(), STREAMING_CODEC_JSON);
    properties.put("tidb.streaming.kafka.bootstrap.servers", "localhost:9092");
    properties.put("tidb.streaming.kafka.topic", "tidb_test");
    properties.put("tidb.streaming.kafka.group.id", "group_1");
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
                  initTiDBCatalog(DST_TABLE, createTableSql, tableEnvironment, properties);
              // create src table
              tiDBCatalog.sqlUpdate(
                  String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, SRC_TABLE));
              tiDBCatalog.sqlUpdate(String.format(createTableSql, DATABASE_NAME, SRC_TABLE));

              TableResult tableResult =
                  tableEnvironment.executeSql(
                      String.format(
                          "INSERT INTO `tidb`.`%s`.`%s` SELECT * FROM `tidb`.`%s`.`%s`",
                          DATABASE_NAME, DST_TABLE, DATABASE_NAME, SRC_TABLE));

              return tableResult.getJobClient();
            });
    executor.submit(task);
    // wait stream job ready
    Thread.sleep(20000);

    JobClient jobClient = task.get().orElseThrow(IllegalStateException::new);

    // insert 3 rows in src, flush 2 insert to dst
    testDatabase.getClientSession().sqlUpdate(String.format(insertSql, DATABASE_NAME, SRC_TABLE));
    Thread.sleep(20000);
    Assert.assertEquals(
        3, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, SRC_TABLE));
    Assert.assertEquals(
        2, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, DST_TABLE));

    // delete 2 rows in src , flush 1 insert and 1 delete to dst
    testDatabase.getClientSession().sqlUpdate(String.format(deleteSql, DATABASE_NAME, SRC_TABLE));
    Thread.sleep(20000);
    Assert.assertEquals(
        1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, SRC_TABLE));
    Assert.assertEquals(
        2, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, DST_TABLE));

    jobClient.cancel();
  }
}
