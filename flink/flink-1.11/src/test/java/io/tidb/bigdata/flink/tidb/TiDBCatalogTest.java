package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class TiDBCatalogTest {

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "default";

  public static final String TABLE_NAME = "test_tidb_type";

  public static final String CREATE_TABLE_SQL = "CREATE TABLE `%s`.`%s`\n"
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

  public static final String INSERT_ROW_SQL = "INSERT INTO `%s`.`%s`.`%s`\n"
      + "VALUES (\n"
      + " cast(1 as tinyint) ,\n"
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
      + " true,\n"
      + " cast('{\"a\":1,\"b\":2}' as string),\n"
      + " cast('1' as string),\n"
      + " cast('a' as string)\n"
      + ")";

  public static String getCreateTableSql(String database, String table) {
    return String.format(CREATE_TABLE_SQL, database, table);
  }

  public static String getDropTableSql(String database, String table) {
    return String.format("DROP TABLE IF EXISTS `%s`.`%s`", database, table);
  }

  public static String getInsertRowSql(String catalog, String database, String table) {
    return String.format(INSERT_ROW_SQL, catalog, database, table);
  }

  @Test
  public void testCatalog() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

    Map<String, String> properties = new HashMap<String, String>() {{
      put(DATABASE_URL, "jdbc:mysql://127.0.0.1:4000/default");
      put(USERNAME, "root");
      put(MAX_POOL_SIZE, "1");
      put(MIN_IDLE_SIZE, "1");
    }};

    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate(getDropTableSql(DATABASE_NAME, TABLE_NAME));
    tiDBCatalog.sqlUpdate(getCreateTableSql(DATABASE_NAME, TABLE_NAME));
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    tableEnvironment.executeSql(getInsertRowSql(CATALOG_NAME, DATABASE_NAME, TABLE_NAME));
    tableEnvironment.executeSql(
        String.format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, TABLE_NAME))
        .print();
  }


}
