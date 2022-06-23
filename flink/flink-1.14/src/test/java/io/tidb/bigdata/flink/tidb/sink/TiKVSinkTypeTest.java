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

import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_PARSE_ERRORS;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_BUFFER_SIZE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_CODEC;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_CODEC_JSON;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import java.util.Map;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * column type - MYSQLTYPE: int - TypeLong, bit-TypeBit, tinyint-TypeTiny, smallint-TypeShort
 * mediumint-TypeInt24, year-TypeYear
 */
@Category(IntegrationTest.class)
public class TiKVSinkTypeTest extends FlinkTestBase {

  private String srcTable;

  private String dstTable;

  private static final String TABLE_TYPE =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  int(11) NOT NULL,\n"
          + "    c2  bit NOT NULL,\n"
          + "    c3  tinyint NOT NULL,\n"
          + "    c4  smallint NOT NULL,\n"
          + "    c5  mediumint NOT NULL,\n"
          + "    c6  year NOT NULL,\n"
          + "    c7  int(11) UNSIGNED NOT NULL,\n"
          + "    PRIMARY KEY (`c1`)\n"
          + ")";

  @Test
  public void testDeduplicate() throws Exception {
    srcTable = "flink_sink_type_src_test" + RandomUtils.randomString();
    dstTable = "flink_sink_type_dst_test" + RandomUtils.randomString();

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
    properties.put(SINK_BUFFER_SIZE.key(), "1");

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // init catalog and create dstTable
    TiDBCatalog tiDBCatalog = initTiDBCatalog(dstTable, TABLE_TYPE, tableEnvironment, properties);
    // create src table
    tiDBCatalog.sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
    tiDBCatalog.sqlUpdate(String.format(TABLE_TYPE, DATABASE_NAME, srcTable));

    TableResult tableResult =
        tableEnvironment.executeSql(
            String.format(
                "INSERT INTO `tidb`.`%s`.`%s` SELECT c1,c2,c3,c4,c5,c6,c7 FROM `tidb`.`%s`.`%s`",
                DATABASE_NAME, dstTable, DATABASE_NAME, srcTable));

    JobClient jobClient = tableResult.getJobClient().orElseThrow(IllegalStateException::new);

    try {
      Thread.sleep(10000);
      // insert (1,1)
      testDatabase
          .getClientSession()
          .sqlUpdate(
              String.format(
                  "insert into `%s`.`%s` values(0,0,0,0,0,2021,2147483648),(1,1,1,1,1,2021,2147483648)",
                  DATABASE_NAME, srcTable));
      Thread.sleep(50000);
      Assert.assertEquals(
          1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, dstTable));
    } finally {
      jobClient.cancel();
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
