package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.IGNORE_AUTOINCREMENT_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.GLOBAL;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK;
import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.PASSWORD;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;
import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.catalog.TiDBCatalog;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

public class FlinkTest {

  public static final String TIDB_HOST = "TIDB_HOST";

  public static final String TIDB_PORT = "TIDB_PORT";

  public static final String TIDB_USER = "TIDB_USER";

  public static final String TIDB_PASSWORD = "TIDB_PASSWORD";

  public static final String tidbHost = getEnvOrDefault(TIDB_HOST, "127.0.0.1");

  public static final String tidbPort = getEnvOrDefault(TIDB_PORT, "4000");

  public static final String tidbUser = getEnvOrDefault(TIDB_USER, "root");

  public static final String tidbPassword = getEnvOrDefault(TIDB_PASSWORD, "");

  private static String getEnvOrDefault(String key, String default0) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return default0;
  }

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "test";

  public static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS `test`";

  public static final String CREATE_TABLE_SQL_FORMAT =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  tinyint,\n"
          + "    c2  smallint,\n"
          + "    c3  mediumint,\n"
          + "    c4  int,\n"
          + "    c5  bigint,\n"
          + "    c6  char(10),\n"
          + "    c7  varchar(20),\n"
          + "    c8  tinytext,\n"
          + "    c9  mediumtext,\n"
          + "    c10 text,\n"
          + "    c11 longtext,\n"
          + "    c12 binary(20),\n"
          + "    c13 varbinary(20),\n"
          + "    c14 tinyblob,\n"
          + "    c15 mediumblob,\n"
          + "    c16 blob,\n"
          + "    c17 longblob,\n"
          + "    c18 float,\n"
          + "    c19 double,\n"
          + "    c20 decimal(6, 3),\n"
          + "    c21 date,\n"
          + "    c22 time,\n"
          + "    c23 datetime,\n"
          + "    c24 timestamp,\n"
          + "    c25 year,\n"
          + "    c26 boolean,\n"
          + "    c27 json,\n"
          + "    c28 enum ('1','2','3'),\n"
          + "    c29 set ('a','b','c'),\n"
          + "    PRIMARY KEY(c1),\n"
          + "    UNIQUE KEY(c2)\n"
          + ")";

  public static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE IF EXISTS `%s`.`%s`";

  // for write mode, only unique key and primary key is mutable.
  public static final String INSERT_ROW_SQL_FORMAT =
      "INSERT INTO `%s`.`%s`.`%s`\n"
          + "VALUES (\n"
          + " cast(%s as tinyint) ,\n"
          + " cast(%s as smallint) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as bigint) ,\n"
          + " cast('chartype' as char(10)),\n"
          + " cast('varchartype' as varchar(20)),\n"
          + " cast('tinytexttype' as string),\n"
          + " cast('mediumtexttype' as string),\n"
          + " cast('texttype' as string),\n"
          + " cast('longtexttype' as string),\n"
          + " cast('binarytype' as bytes),\n"
          + " cast('varbinarytype' as bytes),\n"
          + " cast('tinyblobtype' as bytes),\n"
          + " cast('mediumblobtype' as bytes),\n"
          + " cast('blobtype' as bytes),\n"
          + " cast('longblobtype' as bytes),\n"
          + " cast(1.234 as float),\n"
          + " cast(2.456789 as double),\n"
          + " cast(123.456 as decimal(6,3)),\n"
          + " cast('2020-08-10' as date),\n"
          + " cast('15:30:29' as time),\n"
          + " cast('2020-08-10 15:30:29' as timestamp),\n"
          + " cast('2020-08-10 16:30:29' as timestamp),\n"
          + " cast(2020 as smallint),\n"
          + " cast(true as tinyint),\n"
          + " cast('{\"a\":1,\"b\":2}' as string),\n"
          + " cast('1' as string),\n"
          + " cast('a' as string)\n"
          + ")";

  public static final String CREATE_DATAGEN_TABLE_SQL = "CREATE TABLE datagen (\n"
      + " c1 int,\n"
      + " proctime as PROCTIME()\n"
      + ") WITH (\n"
      + " 'connector' = 'datagen',\n"
      + " 'rows-per-second'='10',\n"
      + " 'fields.c1.kind'='random',\n"
      + " 'fields.c1.min'='1',\n"
      + " 'fields.c1.max'='10',\n"
      + " 'number-of-rows'='10'\n"
      + ")";

  public static String getInsertRowSql(String tableName, byte value1, short value2) {
    return format(INSERT_ROW_SQL_FORMAT, CATALOG_NAME, DATABASE_NAME, tableName, value1, value2);
  }

  public static String getCreateTableSql(String tableName) {
    return String.format(CREATE_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public static String getDropTableSql(String tableName) {
    return String.format(DROP_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public static String getRandomTableName() {
    return UUID.randomUUID().toString().replace("-", "_");
  }

  public Map<String, String> getDefaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(DATABASE_URL,
        String.format(
            "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
            tidbHost, tidbPort));
    properties.put(USERNAME, tidbUser);
    properties.put(PASSWORD, tidbPassword);
    properties.put(MAX_POOL_SIZE, "1");
    properties.put(MIN_IDLE_SIZE, "1");
    return properties;
  }

  public TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inBatchMode().build();
    return TableEnvironment.create(settings);
  }

  public Row runByCatalog(Map<String, String> properties) throws Exception {
    return runByCatalog(properties, null, null);
  }

  public Row runByCatalog(Map<String, String> properties, String resultSql, String tableName)
      throws Exception {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    if (tableName == null) {
      tableName = getRandomTableName();
    }
    String dropTableSql = getDropTableSql(tableName);
    String createTableSql = getCreateTableSql(tableName);
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, dropTableSql, createTableSql);
    // register catalog
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 1));
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 2));
    // query
    if (resultSql == null) {
      resultSql = format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, tableName);
    }
    TableResult tableResult = tableEnvironment.executeSql(resultSql);
    Row row = tableResult.collect().next();
    tiDBCatalog.sqlUpdate(dropTableSql);
    return row;
  }

  public Row copyRow(Row row) {
    Row newRow = new Row(row.getArity());
    for (int i = 0; i < row.getArity(); i++) {
      newRow.setField(i, row.getField(i));
    }
    return newRow;
  }

  public Row copyRow(Row row, int[] indexes) {
    Row newRow = new Row(indexes.length);
    for (int i = 0; i < indexes.length; i++) {
      newRow.setField(i, row.getField(indexes[i]));
    }
    return newRow;
  }

  public Row replicaRead() throws Exception {
    Map<String, String> properties = getDefaultProperties();
    properties.put(TIDB_REPLICA_READ, "follower,leader");
    return runByCatalog(properties);
  }

  public Row upsertAndRead() throws Exception {
    Map<String, String> properties = getDefaultProperties();
    properties.put(TIDB_WRITE_MODE, "upsert");
    return runByCatalog(properties);
  }

  @Test
  public void testTableFactory() throws Exception {
    // only test for timestamp
    Map<String, String> properties = getDefaultProperties();
    properties.put("connector", "tidb");
    properties.put(TiDBOptions.DATABASE_NAME.key(), "test");
    properties.put(TiDBOptions.TABLE_NAME.key(), "test_timestamp");
    properties.put("timestamp-format.c1", "yyyy-MM-dd HH:mm:ss");
    properties.put("timestamp-format.c2", "yyyy-MM-dd HH:mm:ss");
    testTableFactoryWithTimestampFormat(properties);

    properties.put("tidb.timestamp-format.c1", "yyyy-MM-dd HH:mm:ss");
    properties.put("tidb.timestamp-format.c2", "yyyy-MM-dd HH:mm:ss");
    testTableFactoryWithTimestampFormat(properties);

    properties.put("tidb.timestamp-format.c1", "yyyy-MM-dd HH:mm:ss");
    properties.put("timestamp-format.c1", "yyyy-MM-dd HH:mm");
    properties.put("tidb.timestamp-format.c2", "yyyy-MM-dd HH:mm:ss");
    properties.put("timestamp-format.c2", "yyyy-MM-dd HH:mm");
    testTableFactoryWithTimestampFormat(properties);
  }

  private void testTableFactoryWithTimestampFormat(Map<String, String> properties) {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS `test_timestamp`");
    tiDBCatalog.sqlUpdate("CREATE TABLE `test_timestamp`(`c1` VARCHAR(255), `c2` timestamp)",
        "INSERT INTO `test_timestamp` VALUES('2020-01-01 12:00:01','2020-01-01 12:00:02')");
    String propertiesString = properties.entrySet().stream()
        .map(entry -> format("'%s' = '%s'", entry.getKey(), entry.getValue())).collect(
            Collectors.joining(",\n"));
    String createTableSql = format(
        "CREATE TABLE `test_timestamp`(`c1` timestamp, `c2` string) WITH (\n%s\n)",
        propertiesString);
    tableEnvironment.executeSql(createTableSql);
    Row row = tableEnvironment.executeSql("SELECT * FROM `test_timestamp`").collect().next();
    Row row1 = new Row(2);
    row1.setField(0, LocalDateTime.of(2020, 1, 1, 12, 0, 1));
    row1.setField(1, "2020-01-01 12:00:02");
    Assert.assertEquals(row, row1);

    tableEnvironment.executeSql("DROP TABLE `test_timestamp`");
    createTableSql = format(
        "CREATE TABLE `test_timestamp`(`c2` string) WITH (\n%s\n)", propertiesString);
    tableEnvironment.executeSql(createTableSql);
    row = tableEnvironment.executeSql("SELECT * FROM `test_timestamp`").collect().next();
    row1 = new Row(1);
    row1.setField(0, "2020-01-01 12:00:02");
    Assert.assertEquals(row, row1);
    tiDBCatalog.close();
  }

  @Test
  public void testCatalog() throws Exception {
    // read by limit
    String tableName = getRandomTableName();
    Row row = runByCatalog(getDefaultProperties(),
        format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1", CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    // replica read
    Assert.assertEquals(row, replicaRead());
    // upsert and read
    Row row1 = copyRow(row);
    row1.setField(0, (byte) 1);
    row1.setField(1, (short) 2);
    Assert.assertEquals(row1, upsertAndRead());
    // filter push down
    tableName = getRandomTableName();
    Assert.assertEquals(row,
        runByCatalog(getDefaultProperties(),
            format("SELECT * FROM `%s`.`%s`.`%s` WHERE (c1 = 1 OR c3 = 1) AND c2 = 1",
                CATALOG_NAME, DATABASE_NAME, tableName),
            tableName));
    // column pruner
    tableName = getRandomTableName();
    // select 10 column randomly
    Random random = new Random();
    int[] ints = IntStream.range(0, 10).map(i -> random.nextInt(29)).toArray();
    row1 = runByCatalog(getDefaultProperties(),
        format("SELECT %s FROM `%s`.`%s`.`%s` LIMIT 1",
            Arrays.stream(ints).mapToObj(i -> "c" + (i + 1)).collect(Collectors.joining(",")),
            CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    Assert.assertEquals(row1, copyRow(row, ints));
  }

  @Test
  public void testLookupTableSource() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = getDefaultProperties();
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = getRandomTableName();
    String createTableSql1 = String
        .format("CREATE TABLE `%s`.`%s` (c1 int, c2 varchar(255), PRIMARY KEY(`c1`))",
            DATABASE_NAME, tableName);
    String insertDataSql = String
        .format("INSERT INTO `%s`.`%s` VALUES (1,'data1'),(2,'data2'),(3,'data3'),(4,'data4')",
            DATABASE_NAME, tableName);
    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
    String sql = String.format(
        "SELECT * FROM `datagen` "
            + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
            + "ON datagen.c1 = dim_table.c1 ",
        "tidb", DATABASE_NAME, tableName);
    CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Object c1 = row.getField(0);
      String c2 = String.format("data%s", c1);
      boolean isJoin = (int) c1 <= 4;
      Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null, isJoin ? c2 : null);
      Assert.assertEquals(row, row1);
    }
  }

  @Test
  public void testTikvWrite() throws Exception {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    tableEnvironment.getConfig().getConfiguration()
        .setString("table.exec.resource.default-parallelism", "2");
    // create test database and table
    Map<String, String> properties = getDefaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS test_write");
    String createTableSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  tinyint,\n"
            + "    c2  smallint,\n"
            + "    c3  mediumint,\n"
            + "    c4  int,\n"
            + "    c5  bigint,\n"
            + "    c6  char(10),\n"
            + "    c7  varchar(20),\n"
            + "    c8  tinytext,\n"
            + "    c9  mediumtext,\n"
            + "    c10 text,\n"
            + "    c11 longtext,\n"
            + "    c12 binary(20),\n"
            + "    c13 varbinary(20),\n"
            + "    c14 tinyblob,\n"
            + "    c15 mediumblob,\n"
            + "    c16 blob,\n"
            + "    c17 longblob,\n"
            + "    c18 float,\n"
            + "    c19 double,\n"
            + "    c20 decimal(6, 3),\n"
            + "    c21 date,\n"
            + "    c22 time,\n"
            + "    c23 datetime,\n"
            + "    c24 timestamp,\n"
            + "    c25 year,\n"
            + "    c26 boolean,\n"
            + "    c27 json,\n"
            + "    c28 enum ('1','2','3'),\n"
            + "    c29 set ('a','b','c')\n"
            + ")", DATABASE_NAME, "test_write");
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, createTableSql);
    // register catalog
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    String value = "(\n"
        + " cast(1 as tinyint),\n"
        + " cast(1 as smallint) ,\n"
        + " cast(1 as int) ,\n"
        + " cast(1 as int) ,\n"
        + " cast(1 as bigint) ,\n"
        + " cast('chartype' as char(10)),\n"
        + " cast('varchartype' as varchar(20)),\n"
        + " cast('tinytexttype' as string),\n"
        + " cast('mediumtexttype' as string),\n"
        + " cast('texttype' as string),\n"
        + " cast('longtexttype' as string),\n"
        + " cast('binarytype' as bytes),\n"
        + " cast('varbinarytype' as bytes),\n"
        + " cast('tinyblobtype' as bytes),\n"
        + " cast('mediumblobtype' as bytes),\n"
        + " cast('blobtype' as bytes),\n"
        + " cast('longblobtype' as bytes),\n"
        + " cast(1.234 as float),\n"
        + " cast(2.456789 as double),\n"
        + " cast(123.456 as decimal(6,3)),\n"
        + " cast('2020-08-10' as date),\n"
        + " cast('15:30:29' as time),\n"
        + " cast('2020-08-10 15:30:29' as timestamp),\n"
        + " cast('2020-08-10 16:30:29' as timestamp),\n"
        + " cast(2020 as smallint),\n"
        + " cast(true as tinyint),\n"
//        + " cast('{\"a\":1,\"b\":2}' as string),\n"
        + " cast('1' as string)\n"
//        + " cast('a' as string)\n"
        + ")";
    String insertSql = format("INSERT INTO `tidb`.`test`.`test_write`("
        + "`c1`,`c2`,`c3`,`c4`,`c5`,`c6`,`c7`,`c8`,`c9`,`c10`,"
        + "`c11`,`c12`,`c13`,`c14`,`c15`,`c16`,`c17`,`c18`,`c19`,`c20`,"
        + "`c21`,`c22`,`c23`,`c24`,`c25`,`c26`,`c28`"
        + ") "
        + "VALUES%s", String.join(",\n", Collections.nCopies(10, value)));
    tableEnvironment.sqlUpdate(insertSql);
    tableEnvironment.execute("test");
  }

  @Test
  public void testTikvWriteWithUniqueKey() throws Exception {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    tableEnvironment.getConfig().getConfiguration()
        .setString("table.exec.resource.default-parallelism", "2");
    // create test database and table
    Map<String, String> properties = getDefaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS test_write");
    String createTableSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  tinyint,\n"
            + "    c2  smallint,\n"
            + "    c3  mediumint,\n"
            + "    c4  int,\n"
            + "    c5  bigint,\n"
            + "    c6  char(10),\n"
            + "    c7  varchar(20),\n"
            + "    c8  tinytext,\n"
            + "    c9  mediumtext,\n"
            + "    c10 text,\n"
            + "    c11 longtext,\n"
            + "    c12 binary(20),\n"
            + "    c13 varbinary(20),\n"
            + "    c14 tinyblob,\n"
            + "    c15 mediumblob,\n"
            + "    c16 blob,\n"
            + "    c17 longblob,\n"
            + "    c18 float,\n"
            + "    c19 double,\n"
            + "    c20 decimal(6, 3),\n"
            + "    c21 date,\n"
            + "    c22 time,\n"
            + "    c23 datetime,\n"
            + "    c24 timestamp,\n"
            + "    c25 year,\n"
            + "    c26 boolean,\n"
            + "    c27 json,\n"
            + "    c28 enum ('1','2','3'),\n"
            + "    c29 set ('a','b','c'),\n"
            + "    unique key(c1)"
            + ")", DATABASE_NAME, "test_write");
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, createTableSql);
    // register catalog
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    String value = "(\n"
        + " cast(1 as tinyint),\n"
        + " cast(1 as smallint) ,\n"
        + " cast(1 as int) ,\n"
        + " cast(1 as int) ,\n"
        + " cast(1 as bigint) ,\n"
        + " cast('chartype' as char(10)),\n"
        + " cast('varchartype' as varchar(20)),\n"
        + " cast('tinytexttype' as string),\n"
        + " cast('mediumtexttype' as string),\n"
        + " cast('texttype' as string),\n"
        + " cast('longtexttype' as string),\n"
        + " cast('binarytype' as bytes),\n"
        + " cast('varbinarytype' as bytes),\n"
        + " cast('tinyblobtype' as bytes),\n"
        + " cast('mediumblobtype' as bytes),\n"
        + " cast('blobtype' as bytes),\n"
        + " cast('longblobtype' as bytes),\n"
        + " cast(1.234 as float),\n"
        + " cast(2.456789 as double),\n"
        + " cast(123.456 as decimal(6,3)),\n"
        + " cast('2020-08-10' as date),\n"
        + " cast('15:30:29' as time),\n"
        + " cast('2020-08-10 15:30:29' as timestamp),\n"
        + " cast('2020-08-10 16:30:29' as timestamp),\n"
        + " cast(2020 as smallint),\n"
        + " cast(true as tinyint),\n"
//        + " cast('{\"a\":1,\"b\":2}' as string),\n"
        + " cast('1' as string)\n"
//        + " cast('a' as string)\n"
        + ")";
    String insertSql = format("INSERT INTO `tidb`.`test`.`test_write`("
        + "`c1`,`c2`,`c3`,`c4`,`c5`,`c6`,`c7`,`c8`,`c9`,`c10`,"
        + "`c11`,`c12`,`c13`,`c14`,`c15`,`c16`,`c17`,`c18`,`c19`,`c20`,"
        + "`c21`,`c22`,`c23`,`c24`,`c25`,`c26`,`c28`"
        + ") "
        + "VALUES%s", String.join(",\n", Collections.nCopies(10, value)));
    tableEnvironment.sqlUpdate(insertSql);
    tableEnvironment.execute("test");
  }


  @Test
  public void testTikvWriteWithDatagen() throws Exception {
    final int parallelism = 4;
    final int rowCount = 100000;
    final String tableName = "test_write";
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = getDefaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(30000));
    properties.put(UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK.key(), "false");
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%S`", tableName);
    String createTiDBSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  bigint,\n"
            + "    c2  bigint,\n"
            + "    c3  bigint,\n"
            + "    c4  bigint,\n"
            + "    c5  bigint,\n"
            + "    c6  longtext,\n"
            + "    c7  longtext,\n"
            + "    c8  longtext,\n"
            + "    c9  longtext,\n"
            + "    c10 longtext,\n"
            + "    c11 longtext,\n"
            + "    c12 float,\n"
            + "    c13 double,\n"
            + "    c14 date,\n"
            + "    c15 time,\n"
            + "    c16 datetime,\n"
            + "    c17 timestamp\n"
            + ")", DATABASE_NAME, tableName);
    String splitRegionSql = format("SPLIT TABLE `%s` BETWEEN (0) AND (%s) REGIONS %s", tableName,
        20 * 500 * 10000, 20);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql, splitRegionSql);
    CatalogBaseTable table = tiDBCatalog.getTable("test", tableName);
    String createDatagenSql = format("CREATE TABLE datagen \n%s\n WITH (\n"
        + " 'connector' = 'datagen',\n"
        + " 'number-of-rows'='%s'\n"
        + ")", table.getUnresolvedSchema().toString(), rowCount);
    tableEnvironment.executeSql(createDatagenSql);

    tableEnvironment.sqlUpdate(
        "INSERT INTO `tidb`.`test`.`test_write` SELECT * FROM datagen");
    tableEnvironment.execute("test");
  }

  @Test
  public void testTikvWriteWithUniqueIndex() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = getDefaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), "300000");
    properties.put(UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK.key(), "false");
    properties.put(IGNORE_AUTOINCREMENT_COLUMN_VALUE.key(), "true");
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS test_write");
    String createTiDBSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  bigint PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ AUTO_INCREMENT ,\n"
            + "    c2  bigint,\n"
            + "    c3  bigint,\n"
            + "    c4  bigint,\n"
            + "    c5  bigint,\n"
            + "    c6  bigint,\n"
            + "    UNIQUE KEY(c2),\n"
            + "    UNIQUE KEY(c3)\n"
            + ")", DATABASE_NAME, "test_write");
    tiDBCatalog.sqlUpdate(createTiDBSql);
    CatalogBaseTable table = tiDBCatalog.getTable("test", "test_write");
    String createDatagenSql = format("CREATE TABLE datagen \n%s\n WITH (\n"
        + " 'connector' = 'datagen',\n"
        + " 'number-of-rows'='300000'\n"
        + ")", table.getUnresolvedSchema().toString());
    tableEnvironment.executeSql(createDatagenSql);

    tableEnvironment.sqlUpdate(
        "INSERT INTO `tidb`.`test`.`test_write`(c2,c3,c4,c5,c6) SELECT ABS(c2)%1000,ABS(c3)%1000,c4,c5,c6 FROM datagen");
    tableEnvironment.execute("test");
  }

  @Test
  public void kafkaProducer() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    String createDatagenSql = "CREATE TABLE datagen ("
        + "c1 bigint,\n"
        + "c2 bigint) WITH (\n"
        + " 'connector' = 'datagen',\n"
        + " 'number-of-rows'='100000'\n"
        + ")";
    String createKafkaSql = "CREATE TABLE kafka ("
        + "c1 bigint,\n"
        + "c2 bigint"
        + ") WITH (\n"
        + " 'connector' = 'kafka',\n"
        + " 'properties.bootstrap.servers' = 'localhost:9092',\n"
        + " 'topic'='test',\n"
        + " 'format' = 'json'"
        + ")";
    tableEnvironment.executeSql(createDatagenSql);
    tableEnvironment.executeSql(createKafkaSql);

    tableEnvironment.sqlUpdate("INSERT INTO kafka SELECT * FROM datagen");
    tableEnvironment.execute("test");

  }

  @Test
  public void testCheckpoint() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 如果不开 checkpoint，不会 commit，如果是有界流将会 abort，开了就会以固定的时间 commit
    env.enableCheckpointing(10000L);
    env.setParallelism(4);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = getDefaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), "30000");
    properties.put(UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK.key(), "true");
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String createTiDBSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  bigint unique key,\n"
            + "    c2  bigint\n"
            + ")", DATABASE_NAME, "test_write");
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS test_write");
    tiDBCatalog.sqlUpdate(createTiDBSql);
    CatalogBaseTable table = tiDBCatalog.getTable("test", "test_write");
    String createKafkaSql = format("CREATE TABLE kafka \n%s\n WITH (\n"
        + " 'connector' = 'kafka',\n"
        + " 'properties.bootstrap.servers' = 'localhost:9092',\n"
        + " 'topic'='test',\n"
        + " 'scan.startup.mode' = 'latest-offset',\n"
        + " 'format' = 'json'"
        + ")", table.getUnresolvedSchema().toString());
    tableEnvironment.executeSql(createKafkaSql);

    tableEnvironment.sqlUpdate(
        "INSERT INTO `tidb`.`test`.`test_write` SELECT * FROM kafka");
    tableEnvironment.execute("test");
  }


}
