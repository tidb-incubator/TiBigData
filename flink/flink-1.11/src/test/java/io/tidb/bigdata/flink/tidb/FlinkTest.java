package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class FlinkTest {

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "default";

  public static final String TABLE_NAME = "test_tidb_type";

  public static final String CREATE_TABLE_SQL =
      "CREATE TABLE IF NOT EXISTS `default`.`test_tidb_type`\n"
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

  public static final String DROP_TABLE_SQL = format("DROP TABLE IF EXISTS `%s`.`%s`",
      DATABASE_NAME, TABLE_NAME);

  public static final String INSERT_ROW_SQL = "INSERT INTO `tidb`.`default`.`test_tidb_type`\n"
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

  public static String getInsertRowSql(byte value1, short value2) {
    return format(INSERT_ROW_SQL, value1, value2);
  }

  public Map<String, String> getDefaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(DATABASE_URL, "jdbc:mysql://127.0.0.1:4000/default");
    properties.put(USERNAME, "root");
    properties.put(MAX_POOL_SIZE, "1");
    properties.put(MIN_IDLE_SIZE, "1");
    return properties;
  }

  public TableResult runByCatalog(Map<String, String> properties) throws Exception {
    return runByCatalog(properties, null);
  }

  public TableResult runByCatalog(Map<String, String> properties, String resultSql)
      throws Exception {
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    // create test table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate(DROP_TABLE_SQL);
    tiDBCatalog.sqlUpdate(CREATE_TABLE_SQL);
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    tableEnvironment.executeSql(getInsertRowSql((byte) 1, (short) 1));
    tableEnvironment.executeSql(getInsertRowSql((byte) 1, (short) 2));
    // query
    if (resultSql == null) {
      resultSql = format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, TABLE_NAME);
    }
    return tableEnvironment.executeSql(resultSql);
  }

  public Row copyRow(Row row) {
    Row newRow = new Row(row.getArity());
    for (int i = 0; i < row.getArity(); i++) {
      newRow.setField(i, row.getField(i));
    }
    return newRow;
  }

  public Row getResultRow(String resultSql) throws Exception {
    return runByCatalog(getDefaultProperties(), resultSql).collect().next();
  }

  public Row replicaRead() throws Exception {
    Map<String, String> properties = getDefaultProperties();
    properties.put(TIDB_REPLICA_READ, "true");
    return runByCatalog(properties).collect().next();
  }

  public Row upsertAndRead() throws Exception {
    Map<String, String> properties = getDefaultProperties();
    properties.put(TIDB_WRITE_MODE, "upsert");
    return runByCatalog(properties).collect().next();
  }

  @Test
  public void testCatalog() throws Exception {
    // read
    Row row = getResultRow(null);
    // replica read
    Assert.assertEquals(row, replicaRead());
    // upsert and read
    row.setField(0, (byte) 1);
    row.setField(1, (short) 2);
    Assert.assertEquals(row, upsertAndRead());
  }

}
