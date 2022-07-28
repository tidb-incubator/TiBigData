package io.tidb.bigdata.flink.tidb.insert;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TiKVInsertCodecTest extends FlinkTestBase {
  String table;

  @Test
  public void testInsertCodec() throws Exception {
    table = "flink_insert_dst_test" + RandomUtils.randomString();
    String createTable =
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  varchar(255),\n"
            + "    c2  varchar(255) default null,\n"
            + "    PRIMARY KEY (`c1`) CLUSTERED, \n"
            + "    UNIQUE KEY(`c2`) \n"
            + ")";
    Map<String, String> properties = defaultProperties();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    TableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    // init catalog and create table
    TiDBCatalog tiDBCatalog = initTiDBCatalog(table, createTable, tableEnvironment, properties);
    tableEnvironment.sqlUpdate(
        String.format(
            "insert into `tidb`.`%s`.`%s` values('1','1'),('2',cast(null as varchar))",
            DATABASE_NAME, table));
    tableEnvironment.execute("test");
    Assert.assertEquals(2, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, table));
    tableEnvironment.sqlUpdate(
        String.format(
            "insert into `tidb`.`%s`.`%s` values('3','1'),('4',cast(null as varchar))",
            DATABASE_NAME, table));
    tableEnvironment.execute("test");
    Assert.assertEquals(3, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, table));
  }

  @Test
  public void testUniqueIndexValueEncode() throws Exception {
    table = "flink_insert_dst_test" + RandomUtils.randomString();
    String createTable =
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  varchar(255),\n"
            + "    c2  varchar(255) default null,\n"
            + "    PRIMARY KEY (`c1`) CLUSTERED, \n"
            + "    UNIQUE KEY(`c2`) \n"
            + ")";

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    TableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), SinkTransaction.MINIBATCH.name());
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    // init catalog and create table
    TiDBCatalog tiDBCatalog = initTiDBCatalog(table, createTable, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "insert into `tidb`.`%s`.`%s` values('1',cast(null as varchar))",
            DATABASE_NAME, table));
    tableEnvironment.execute("test");
    Assert.assertEquals(1, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, table));
    tableEnvironment.sqlUpdate(
        String.format(
            "insert into `tidb`.`%s`.`%s` values('2',cast(null as varchar))",
            DATABASE_NAME, table));
    tableEnvironment.execute("test");
    Assert.assertEquals(2, testDatabase.getClientSession().queryTableCount(DATABASE_NAME, table));
  }

  @After
  public void teardown() {
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, table));
  }
}
