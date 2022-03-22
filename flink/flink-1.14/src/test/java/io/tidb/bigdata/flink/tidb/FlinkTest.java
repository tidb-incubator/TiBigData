package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DEDUPLICATE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_BUFFER_SIZE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.CHECKPOINT;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.GLOBAL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.test.TableUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.ObjectRowImpl;

@Category(IntegrationTest.class)
public class FlinkTest {

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

  public TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
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
      tableName = RandomUtils.randomString();
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
    Map<String, String> properties = defaultProperties();
    properties.put(TIDB_REPLICA_READ, "follower,leader");
    return runByCatalog(properties);
  }

  public Row upsertAndRead() throws Exception {
    Map<String, String> properties = defaultProperties();
    properties.put(TIDB_WRITE_MODE, "upsert");
    return runByCatalog(properties);
  }

  @Test
  public void testCatalog() throws Exception {
    // read by limit
    String tableName = RandomUtils.randomString();
    Row row = runByCatalog(defaultProperties(),
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
    tableName = RandomUtils.randomString();
    Assert.assertEquals(row,
        runByCatalog(defaultProperties(),
            format("SELECT * FROM `%s`.`%s`.`%s` WHERE (c1 = 1 OR c3 = 1) AND c2 = 1",
                CATALOG_NAME, DATABASE_NAME, tableName),
            tableName));
    // column pruner
    tableName = RandomUtils.randomString();
    // select 10 column randomly
    Random random = new Random();
    int[] ints = IntStream.range(0, 10).map(i -> random.nextInt(29)).toArray();
    row1 = runByCatalog(defaultProperties(),
        format("SELECT %s FROM `%s`.`%s`.`%s` LIMIT 1",
            Arrays.stream(ints).mapToObj(i -> "c" + (i + 1)).collect(Collectors.joining(",")),
            CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    Assert.assertEquals(row1, copyRow(row, ints));
  }

  @Test
  public void testLookupTableSource() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = RandomUtils.randomString();
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

  private void generateData(String tableName, int rowCount) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(10000));
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
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    CatalogBaseTable table = tiDBCatalog.getTable("test", tableName);
    String createDatagenSql = format("CREATE TABLE datagen \n%s\n WITH (\n"
        + " 'connector' = 'datagen',\n"
        + " 'number-of-rows'='%s',\n"
        + " 'fields.c1.kind'='sequence',\n"
        + " 'fields.c1.start'='1',\n"
        + " 'fields.c1.end'='%s'\n"
        + ")", table.getUnresolvedSchema().toString(), rowCount, rowCount);
    tableEnvironment.executeSql(createDatagenSql);
    String sql = format("INSERT INTO `tidb`.`test`.`%s` SELECT * FROM datagen", tableName);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    // splits
    String splitRegionSql = format("SPLIT TABLE `%s` BETWEEN (0) AND (%s) REGIONS %s", tableName,
        rowCount * 8, 100);
    tiDBCatalog.sqlUpdate(splitRegionSql);
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, tableName));
  }

  public static class CheckpointMapFunction implements MapFunction<Long, RowData> {

    private static final Random RANDOM = new Random();
    private static final Set<Long> randomLong = new HashSet<>();

    static {
      while (randomLong.size() < 3) {
        randomLong.add((long) RANDOM.nextInt(100));
      }
    }

    public CheckpointMapFunction() {
      System.out.println("----------------------------");
      System.out.println(randomLong);
      System.out.println("----------------------------");
    }

    @Override
    public RowData map(Long value) throws Exception {
      GenericRowData rowData = new GenericRowData(1);
      rowData.setField(0, value);
      // Waiting for checkpoint
      Thread.sleep(50L);
      if (randomLong.remove(value)) {
        System.out.println("----------------------------" + value);
        throw new IllegalStateException();
      }
      return rowData;
    }
  }

  @Test
  public void testWrite() {
    ClientSession clientSession = ClientSession.create(
        new ClientConfig(ConfigUtils.defaultProperties()));
    String tableName = RandomUtils.randomString();
    String databaseName = "test";
    clientSession.sqlUpdate(String.format("CREATE TABLE IF NOT EXISTS `%s`\n"
        + "(\n"
        + "    c1  bigint,\n"
        + "    UNIQUE KEY(c1)"
        + ")", tableName));
    writeData(clientSession, tableName, databaseName);
    writeData(clientSession, tableName, databaseName);
  }

  private void writeData(ClientSession clientSession, String tableName, String databaseName) {
    TiTimestamp timestamp = clientSession.getSnapshotVersion();
    DynamicRowIDAllocator rowIDAllocator = new DynamicRowIDAllocator(clientSession,
        databaseName, tableName, 100);
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(clientSession, timestamp, databaseName,
        tableName, false, true, rowIDAllocator);
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(clientSession.getTiSession(),
        timestamp.getVersion());
    List<BytePairWrapper> pairs = LongStream.range(0, 1000)
        .mapToObj(i -> ObjectRowImpl.create(new Long[]{i}))
        .map(tiDBEncodeHelper::generateKeyValuesByRow)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
    tiDBWriteHelper.preWriteFirst(pairs);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }

  @Test
  public void testMiniBatchInCheckpoint() throws Exception {
    final String srcTable = "source_table";
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100L);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
        Integer.MAX_VALUE, Time.of(1, TimeUnit.SECONDS)));
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), "12345");
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");
    properties.put(SINK_BUFFER_SIZE.key(), "1");
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String createTiDBSql = String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
            + "(\n"
            + "    c1  bigint,\n"
            + "    unique key(c1)\n"
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`", dstTable));
    tiDBCatalog.sqlUpdate(createTiDBSql);
    NumberSequenceSource numberSequenceSource = new NumberSequenceSource(1, 1000);
    TableSchema tableSchema = TableSchema.builder().field("c1", DataTypes.BIGINT()).build();
    TypeInformation<RowData> typeInformation = (TypeInformation<RowData>) ScanRuntimeProviderContext
        .INSTANCE.createTypeInformation(tableSchema.toRowDataType());
    SingleOutputStreamOperator<RowData> dataStream = env.fromSource(numberSequenceSource,
            WatermarkStrategy.noWatermarks(), "number-source")
        .map(new CheckpointMapFunction())
        .returns(typeInformation);
    tableEnvironment.createTemporaryView(srcTable, dataStream);
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`test`.`%s` SELECT c1 FROM %s WHERE c1 <= 100",
            dstTable, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(100, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testMiniBatchTransaction() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(30000));
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%S`", dstTable);
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
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    tableEnvironment.sqlUpdate(String.format("INSERT INTO `tidb`.`test`.`%s` "
        + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
        + "FROM `tidb`.`test`.`%s`", dstTable, srcTable));
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testMiniBatchTransactionWithIndex() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), MINIBATCH.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(30000));
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");

    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%s`", dstTable);
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
            + "    c17 timestamp,\n"
            + "    unique key(c1)\n"
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    String sql = format("INSERT INTO `tidb`.`test`.`%s` "
        + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
        + "FROM `tidb`.`test`.`%s`", dstTable, srcTable);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testGlobalTransaction() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(10000));
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%S`", dstTable);
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
            + "    c17 timestamp,\n"
            + "    unique key(c1)\n"
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    String sql = format("INSERT INTO `tidb`.`test`.`%s` "
        + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
        + "FROM `tidb`.`test`.`%s`", dstTable, srcTable);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testGlobalTransactionWithIndex() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), GLOBAL.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(30000));
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");

    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%s`", dstTable);
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
            + "    c17 timestamp,\n"
            + "    unique key(c1)\n"
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    String sql = format("INSERT INTO `tidb`.`test`.`%s` "
        + "SELECT ABS(c1%%2000),c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
        + "FROM `tidb`.`test`.`%s`", dstTable, srcTable);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    Assert.assertEquals(2000, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testSqlHint() throws Exception {
    final int rowCount = 100000;
    final String srcTable = RandomUtils.randomString();
    generateData(srcTable, rowCount);
    final String dstTable = RandomUtils.randomString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), CHECKPOINT.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), Integer.toString(10000));
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    String dropTableSql = format("DROP TABLE IF EXISTS `%S`", dstTable);
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
            + "    c17 timestamp,\n"
            + "    unique key(c1)\n"
            + ")", DATABASE_NAME, dstTable);
    tiDBCatalog.sqlUpdate(dropTableSql, createTiDBSql);
    String sql = format(
        "INSERT INTO `tidb`.`test`.`%s` /*+ OPTIONS('tikv.sink.transaction'='global') */"
            + "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 "
            + "FROM `tidb`.`test`.`%s`", dstTable, srcTable);
    System.out.println(sql);
    tableEnvironment.sqlUpdate(sql);
    tableEnvironment.execute("test");
    Assert.assertEquals(rowCount, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  /**
   * This test will never end, we need find another way to test checkpoint.
   */
  @Ignore
  public void testCheckpoint() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10000L);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), CHECKPOINT.name());
    properties.put(ROW_ID_ALLOCATOR_STEP.key(), "12345");
    properties.put(DEDUPLICATE.key(), "true");
    properties.put(WRITE_MODE.key(), "upsert");
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
    String createDatagenSql = format("CREATE TABLE datagen \n%s\n WITH (\n"
        + " 'connector' = 'datagen',\n"
        + " 'rows-per-second' = '1000'"
        + ")", table.getUnresolvedSchema().toString());
    tableEnvironment.executeSql(createDatagenSql);

    tableEnvironment.sqlUpdate(
        "INSERT INTO `tidb`.`test`.`test_write` SELECT c1,c2 FROM datagen");
    tableEnvironment.execute("test");
  }

  @Test
  public void testSnapshotRead() throws Exception {
    for (int i = 1; i <= 3; i++) {
      // insert
      Map<String, String> properties = ConfigUtils.defaultProperties();
      ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
      String tableName = RandomUtils.randomString();
      clientSession.sqlUpdate(String.format("CREATE TABLE `%s` (`c1` int,`c2` int)", tableName),
          String.format("INSERT INTO `%s` VALUES(1,1)", tableName));

      if (i == 1) {
        // get timestamp
        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
            ZonedDateTime.now().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        // wait for 1 second, because we use client time rather than server time
        Thread.sleep(1000L);
      } else {
        // get version
        long version = clientSession.getSnapshotVersion().getVersion();
        properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
      }

      // update
      clientSession.sqlUpdate(String.format("UPDATE `%s` SET c1 = 2 WHERE c1 =1", tableName));

      if (i == 3) {
        // get timestamp
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
            zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
      }

      // read by version
      TableEnvironment tableEnvironment = getTableEnvironment();
      properties.put("type", "tidb");
      String createCatalogSql = format("CREATE CATALOG `tidb` WITH ( %s )",
          TableUtils.toSqlProperties(properties));
      tableEnvironment.executeSql(createCatalogSql);
      String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s`", "tidb", DATABASE_NAME,
          tableName);
      CloseableIterator<Row> iterator = tableEnvironment.executeSql(queryTableSql).collect();
      while (iterator.hasNext()) {
        Row row = iterator.next();
        Assert.assertEquals(Row.of(1, 1), row);
      }
      iterator.close();
    }
  }
}
