package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.PASSWORD;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;
import static java.lang.String.format;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
          + " true,\n"
          + " cast('{\"a\":1,\"b\":2}' as string),\n"
          + " cast('1' as string),\n"
          + " cast('a' as string)\n"
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
    return runByCatalog(properties, null);
  }

  public Row runByCatalog(Map<String, String> properties, String resultSql) {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = getRandomTableName();
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

  public Row replicaRead() throws Exception {
    Map<String, String> properties = getDefaultProperties();
    properties.put(TIDB_REPLICA_READ, "true");
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
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    Map<String, String> properties = getDefaultProperties();
    properties.put("connector", "tidb");
    properties.put(TiDBDynamicTableFactory.DATABASE_NAME.key(), "test");
    properties.put(TiDBDynamicTableFactory.TABLE_NAME.key(), "test_timestamp");
    properties.put("timestamp-format.c1", "yyyy-MM-dd HH:mm:ss");
    properties.put("timestamp-format.c2", "yyyy-MM-dd HH:mm:ss");
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
    // read
    Row row = runByCatalog(getDefaultProperties());
    // replica read
    Assert.assertEquals(row, replicaRead());
    // upsert and read
    row.setField(0, (byte) 1);
    row.setField(1, (short) 2);
    Assert.assertEquals(row, upsertAndRead());
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
    List<Row> rows = IntStream.range(1, 11).mapToObj(Row::of)
        .collect(Collectors.toList());
    TableSchema tableSchema = TableSchema.builder().field("c1", DataTypes.INT().notNull()).build();
    Table table = tableEnvironment.fromValues(tableSchema.toRowDataType(), rows);
    tableEnvironment.registerTable("data", table);
    String sql = String.format(
        "SELECT * FROM (SELECT c1,PROCTIME() AS proctime FROM data) AS `datagen` "
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

}
