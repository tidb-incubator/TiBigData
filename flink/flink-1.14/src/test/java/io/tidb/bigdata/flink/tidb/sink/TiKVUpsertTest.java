package io.tidb.bigdata.flink.tidb.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DEDUPLICATE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.RandomUtils;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Assert;
import org.junit.Test;

public class TiKVUpsertTest extends FlinkTestBase {

  private static final String TABLE =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "("
          + "  `id` bigint(20) NOT NULL,\n"
          + "  `name` varchar(255) NULL DEFAULT NULL,\n"
          + "  PRIMARY KEY (`id`) USING BTREE\n"
          + ")";

  @Test
  public void testMiniBatchTransaction() throws Exception {
    String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");

    TiDBCatalog tiDBCatalog = initTiDBCatalog(dstTable, TABLE, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "VALUES(1, 'before')", DATABASE_NAME, dstTable));
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "VALUES(1, 'after')", DATABASE_NAME, dstTable));
    tableEnvironment.execute("test");

    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, after]"), dstTable);
  }
}
