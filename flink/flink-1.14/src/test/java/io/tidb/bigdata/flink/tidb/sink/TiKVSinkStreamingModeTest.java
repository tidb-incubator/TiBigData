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
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_BUFFER_SIZE;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class TiKVSinkStreamingModeTest extends FlinkTestBase {

  private static final String SCHEME_ONE_UNIQUE_KEY =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n" + "(\n" + "    c1  bigint,\n"
          + "    unique key(c1)\n" + ")";

  @Parameters(name = "{index}: Transaction={0}, WriteMode={1}, Deduplicate={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{SinkTransaction.MINIBATCH, TiDBWriteMode.UPSERT, true}});
  }

  private final SinkTransaction transaction;
  private final TiDBWriteMode writeMode;
  private final boolean deduplicate;

  public TiKVSinkStreamingModeTest(SinkTransaction transaction, TiDBWriteMode writeMode,
      boolean deduplicate) {
    this.transaction = transaction;
    this.writeMode = writeMode;
    this.deduplicate = deduplicate;
  }

  @Test
  public void testStreamingModeFailRecovery() throws Exception {
    final String srcTable = "source_table";
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100L);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(1, TimeUnit.SECONDS)));
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), transaction.name());
    properties.put(DEDUPLICATE.key(), String.valueOf(deduplicate));
    properties.put(WRITE_MODE.key(), writeMode.name());
    properties.put(SINK_BUFFER_SIZE.key(), "1");

    TiDBCatalog tiDBCatalog = initTiDBCatalog(dstTable, SCHEME_ONE_UNIQUE_KEY, tableEnvironment,
        properties);

    NumberSequenceSource numberSequenceSource = new NumberSequenceSource(1, 1000);
    TableSchema tableSchema = TableSchema.builder().field("c1", DataTypes.BIGINT()).build();

    TypeInformation<RowData> typeInformation = (TypeInformation<RowData>) ScanRuntimeProviderContext.INSTANCE.createTypeInformation(
        tableSchema.toRowDataType());
    SingleOutputStreamOperator<RowData> dataStream = env.fromSource(numberSequenceSource,
            WatermarkStrategy.noWatermarks(), "number-source").map(new RandomExceptionMapFunction())
        .returns(typeInformation);
    tableEnvironment.createTemporaryView(srcTable, dataStream);
    tableEnvironment.sqlUpdate(
        String.format("INSERT INTO `tidb`.`%s`.`%s` SELECT c1 FROM %s WHERE c1 <= 100", DATABASE_NAME, dstTable,
            srcTable));
    tableEnvironment.execute("test");

    Assert.assertEquals(100, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  public static class RandomExceptionMapFunction implements MapFunction<Long, RowData> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final Random RANDOM = new Random();
    private static final Set<Long> randomLong = new HashSet<>();

    static {
      while (randomLong.size() < 3) {
        randomLong.add((long) RANDOM.nextInt(100));
      }
    }

    public RandomExceptionMapFunction() {
      logger.info("----------------------------");
      logger.info(String.valueOf(randomLong));
      logger.info("----------------------------");
    }

    @Override
    public RowData map(Long value) throws Exception {
      GenericRowData rowData = new GenericRowData(1);
      rowData.setField(0, value);
      // Waiting for checkpoint
      Thread.sleep(50L);
      if (randomLong.remove(value)) {
        logger.info("----------------------------" + value);
        throw new IllegalStateException("Mock exception");
      }
      return rowData;
    }
  }

}