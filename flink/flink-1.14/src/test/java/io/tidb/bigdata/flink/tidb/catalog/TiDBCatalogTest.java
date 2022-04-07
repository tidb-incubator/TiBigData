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

package io.tidb.bigdata.flink.tidb.catalog;

import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static java.lang.String.format;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;

import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TiDBCatalogTest extends FlinkTestBase {

  private static final String CREATE_TABLE_SQL_FORMAT =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n" + "(\n" + "    c1  tinyint,\n"
          + "    c2  smallint,\n" + "    c3  mediumint,\n" + "    c4  int,\n" + "    c5  bigint,\n"
          + "    c6  char(10),\n" + "    c7  varchar(20),\n" + "    c8  tinytext,\n"
          + "    c9  mediumtext,\n" + "    c10 text,\n" + "    c11 longtext,\n"
          + "    c12 binary(20),\n" + "    c13 varbinary(20),\n" + "    c14 tinyblob,\n"
          + "    c15 mediumblob,\n" + "    c16 blob,\n" + "    c17 longblob,\n" + "    c18 float,\n"
          + "    c19 double,\n" + "    c20 decimal(6, 3),\n" + "    c21 date,\n" + "    c22 time,\n"
          + "    c23 datetime,\n" + "    c24 timestamp,\n" + "    c25 year,\n"
          + "    c26 boolean,\n" + "    c27 json,\n" + "    c28 enum ('1','2','3'),\n"
          + "    c29 set ('a','b','c'),\n" + "    PRIMARY KEY(c1),\n" + "    UNIQUE KEY(c2)\n"
          + ")";

  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE IF EXISTS `%s`.`%s`";

  // for write mode, only unique key and primary key is mutable.
  private static final String INSERT_ROW_SQL_FORMAT =
      "INSERT INTO `%s`.`%s`.`%s`\n" + "VALUES (\n" + " cast(%s as tinyint) ,\n"
          + " cast(%s as smallint) ,\n" + " cast(1 as int) ,\n" + " cast(1 as int) ,\n"
          + " cast(1 as bigint) ,\n" + " cast('chartype' as char(10)),\n"
          + " cast('varchartype' as varchar(20)),\n" + " cast('tinytexttype' as string),\n"
          + " cast('mediumtexttype' as string),\n" + " cast('texttype' as string),\n"
          + " cast('longtexttype' as string),\n" + " cast('binarytype' as bytes),\n"
          + " cast('varbinarytype' as bytes),\n" + " cast('tinyblobtype' as bytes),\n"
          + " cast('mediumblobtype' as bytes),\n" + " cast('blobtype' as bytes),\n"
          + " cast('longblobtype' as bytes),\n" + " cast(1.234 as float),\n"
          + " cast(2.456789 as double),\n" + " cast(123.456 as decimal(6,3)),\n"
          + " cast('2020-08-10' as date),\n" + " cast('15:30:29' as time),\n"
          + " cast('2020-08-10 15:30:29' as timestamp),\n"
          + " cast('2020-08-10 16:30:29' as timestamp),\n" + " cast(2020 as smallint),\n"
          + " cast(true as tinyint),\n" + " cast('{\"a\":1,\"b\":2}' as string),\n"
          + " cast('1' as string),\n" + " cast('a' as string)\n" + ")";

  @Test
  public void testCatalog() throws Exception {
    // read by limit
    String tableName = RandomUtils.randomString();
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_BUFFER_FLUSH_MAX_ROWS.key(), "1");

    Row row = runByCatalog(properties,
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
    Assert.assertEquals(row, runByCatalog(properties,
        format("SELECT * FROM `%s`.`%s`.`%s` WHERE (c1 = 1 OR c3 = 1) AND c2 = 1", CATALOG_NAME,
            DATABASE_NAME, tableName), tableName));
    // column pruner
    tableName = RandomUtils.randomString();
    // select 10 column randomly
    Random random = new Random();
    int[] ints = IntStream.range(0, 10).map(i -> random.nextInt(29)).toArray();
    row1 = runByCatalog(properties, format("SELECT %s FROM `%s`.`%s`.`%s` LIMIT 1",
        Arrays.stream(ints).mapToObj(i -> "c" + (i + 1)).collect(Collectors.joining(",")),
        CATALOG_NAME, DATABASE_NAME, tableName), tableName);
    Assert.assertEquals(row1, copyRow(row, ints));
  }

  private static String getInsertRowSql(String tableName, byte value1, short value2) {
    return format(INSERT_ROW_SQL_FORMAT, CATALOG_NAME, DATABASE_NAME, tableName, value1, value2);
  }

  private static String getCreateTableSql(String tableName) {
    return String.format(CREATE_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  private static String getDropTableSql(String tableName) {
    return String.format(DROP_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  private Row runByCatalog(Map<String, String> properties) throws Exception {
    return runByCatalog(properties, null, null);
  }

  private Row runByCatalog(Map<String, String> properties, String resultSql, String tableName)
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
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 1)).await();
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 2)).await();
    // query
    if (resultSql == null) {
      resultSql = format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, tableName);
    }
    TableResult tableResult = tableEnvironment.executeSql(resultSql);
    Row row;
    try (CloseableIterator<Row> iterator = tableResult.collect()) {
      row = iterator.next();
    }
    tiDBCatalog.sqlUpdate(dropTableSql);
    return row;
  }

  private Row copyRow(Row row) {
    Row newRow = new Row(row.getArity());
    for (int i = 0; i < row.getArity(); i++) {
      newRow.setField(i, row.getField(i));
    }
    return newRow;
  }

  private Row copyRow(Row row, int[] indexes) {
    Row newRow = new Row(indexes.length);
    for (int i = 0; i < indexes.length; i++) {
      newRow.setField(i, row.getField(indexes[i]));
    }
    return newRow;
  }

  private Row replicaRead() throws Exception {
    Map<String, String> properties = defaultProperties();
    properties.put(TIDB_REPLICA_READ, "follower,leader");
    return runByCatalog(properties);
  }

  private Row upsertAndRead() throws Exception {
    Map<String, String> properties = defaultProperties();
    properties.put(TIDB_WRITE_MODE, "upsert");
    return runByCatalog(properties);
  }
}