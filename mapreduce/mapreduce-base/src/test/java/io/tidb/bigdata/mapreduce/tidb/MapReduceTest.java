/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.mapreduce.tidb;

import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.mapreduce.tidb.example.TiDBMapreduceDemo;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.handle.TableHandleInternal;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MapReduceTest {

  public static final String DATABASE_NAME = "test";

  public static final String TABLE_NAME = "mapreduce_test_table";

  public static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME;

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

  public static final String INSERT_ROW_SQL_FORMAT =
      "INSERT INTO `%s`.`%s`\n"
          + "VALUES (\n"
          + " 1 ,\n"
          + " 1 ,\n"
          + " 1 ,\n"
          + " 1 ,\n"
          + " 1 ,\n"
          + " 'chartype' ,\n"
          + " 'varchartype'  ,\n"
          + " 'tinytexttype' ,\n"
          + " 'mediumtexttype' ,\n"
          + " 'texttype' ,\n"
          + " 'longtexttype' ,\n"
          + " 'binarytype' ,\n"
          + " 'varbinarytype' ,\n"
          + " 'tinyblobtype' ,\n"
          + " 'mediumblobtype' ,\n"
          + " 'blobtype' ,\n"
          + " 'longblobtype' ,\n"
          + " 1.234 ,\n"
          + " 2.456789 ,\n"
          + " 123.456 ,\n"
          + " '2020-08-10' ,\n"
          + " '15:30:29' ,\n"
          + " '2020-08-10 15:30:29' ,\n"
          + " '2020-08-10 16:30:29' ,\n"
          + " 2020 ,\n"
          + " true ,\n"
          + " '{\"a\":1,\"b\":2}' ,\n"
          + " '1' ,\n"
          + " 'a' \n"
          + ")";

  public static String getInsertRowSql(String tableName) {
    return format(INSERT_ROW_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public static String getCreateTableSql(String tableName) {
    return String.format(CREATE_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  public static String getDropTableSql(String tableName) {
    return String.format(DROP_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }

  private static String getCreateDatabaseSql(String database) {
    return String.format(CREATE_DATABASE_SQL, database);
  }

  private static void doUpdateSql(Connection con, String updateSql) throws SQLException {
    try (PreparedStatement ps = con.prepareStatement(updateSql)) {
      ps.executeUpdate();
    }
  }

  public ClientSession getSingleConnection() {
    return ClientSession.create(new ClientConfig(ConfigUtils.defaultProperties()));
  }

  @Before
  public void before() throws Exception {
    try (ClientSession clientSession = getSingleConnection();
        Connection connection = clientSession.getJdbcConnection()) {
      doUpdateSql(connection, getCreateDatabaseSql(DATABASE_NAME));
      doUpdateSql(connection, getDropTableSql(TABLE_NAME));
      doUpdateSql(connection, getCreateTableSql(TABLE_NAME));
      doUpdateSql(connection, getInsertRowSql(TABLE_NAME));
    }
  }

  @Test
  public void testReadRecords() throws Exception {
    ClientSession clientSession = getSingleConnection();
    TiTableInfo tiTableInfo = clientSession.getTableMust(DATABASE_NAME, TABLE_NAME);
    TableHandleInternal tableHandleInternal = new TableHandleInternal(DATABASE_NAME, tiTableInfo);
    List<SplitInternal> splitInternals = clientSession.getSplits(tableHandleInternal);
    List<ColumnHandleInternal> columnHandleInternals = ClientSession.getTableColumns(tiTableInfo);

    for (SplitInternal splitInternal : splitInternals) {
      List<ColumnHandleInternal> columns =
          Arrays.stream(IntStream.range(0, 29).toArray())
              .mapToObj(columnHandleInternals::get)
              .collect(Collectors.toList());
      RecordCursorInternal cursor =
          RecordSetInternal.builder(clientSession, ImmutableList.of(splitInternal), columns)
              .withExpression(null)
              .withTimestamp(null)
              .withLimit(null)
              .withQueryHandle(false)
              .build()
              .cursor();
      cursor.advanceNextPosition();
      TiDBResultSet tiDBResultSet = new TiDBResultSet(cursor.fieldCount(), null);
      for (int index = 0; index < cursor.fieldCount(); index++) {
        Object object = cursor.getObject(index);
        tiDBResultSet.setObject(object, index + 1);
      }
      Integer c1 = tiDBResultSet.getInt(1);
      Integer c2 = tiDBResultSet.getInt(2);
      Integer c3 = tiDBResultSet.getInt(3);
      Integer c4 = tiDBResultSet.getInt(4);
      Long c5 = tiDBResultSet.getLong(5);
      String c6 = tiDBResultSet.getString(6);
      String c7 = tiDBResultSet.getString(7);
      String c8 = tiDBResultSet.getString(8);
      String c9 = tiDBResultSet.getString(9);
      String c10 = tiDBResultSet.getString(10);
      String c11 = tiDBResultSet.getString(11);
      byte[] c12 = tiDBResultSet.getBytes(12);
      byte[] c13 = tiDBResultSet.getBytes(13);
      byte[] c14 = tiDBResultSet.getBytes(14);
      byte[] c15 = tiDBResultSet.getBytes(15);
      byte[] c16 = tiDBResultSet.getBytes(16);
      byte[] c17 = tiDBResultSet.getBytes(17);
      Float c18 = tiDBResultSet.getFloat(18);
      Double c19 = tiDBResultSet.getDouble(19);
      BigDecimal c20 = tiDBResultSet.getBigDecimal(20);
      Date c21 = tiDBResultSet.getDate(21);
      Time c22 = tiDBResultSet.getTime(22);
      Timestamp c23 = tiDBResultSet.getTimestamp(23);
      Timestamp c24 = tiDBResultSet.getTimestamp(24);
      Integer c25 = tiDBResultSet.getInt(25);
      boolean c26 = tiDBResultSet.getBoolean(26);
      String c27 = tiDBResultSet.getString(27);
      String c28 = tiDBResultSet.getString(28);
      String c29 = tiDBResultSet.getString(29);

      Assert.assertEquals(new Integer(1), c1);
      Assert.assertEquals(new Integer(1), c2);
      Assert.assertEquals(new Integer(1), c3);
      Assert.assertEquals(new Integer(1), c4);
      Assert.assertEquals(new Long(1), c5);
      Assert.assertEquals("chartype", c6);
      Assert.assertEquals("varchartype", c7);
      Assert.assertEquals("tinytexttype", c8);
      Assert.assertEquals("mediumtexttype", c9);
      Assert.assertEquals("texttype", c10);
      Assert.assertEquals("longtexttype", c11);
      Assert.assertEquals("binarytype", new String(c12, 0, "binarytype".length()));
      Assert.assertEquals("varbinarytype", new String(c13, 0, "varbinarytype".length()));
      Assert.assertEquals("tinyblobtype", new String(c14));
      Assert.assertEquals("mediumblobtype", new String(c15));
      Assert.assertEquals("blobtype", new String(c16));
      Assert.assertEquals("longblobtype", new String(c17));
      Assert.assertEquals(new Float(1.234), c18);
      Assert.assertEquals(new Double(2.456789), c19);
      Assert.assertEquals("123.456", c20.toString());
      Assert.assertEquals("2020-08-10", c21.toString());
      Assert.assertEquals("15:30:29", c22.toString());
      Assert.assertEquals("2020-08-10 15:30:29.0", c23.toString());
      Assert.assertEquals("2020-08-10 16:30:29.0", c24.toString());
      Assert.assertEquals(new Integer(2020), c25);
      Assert.assertTrue(c26);
      Assert.assertEquals("{\"a\":1,\"b\":2}", c27);
      Assert.assertEquals("1", c28);
      Assert.assertEquals("a", c29);
    }

    clientSession.close();
  }

  private long getMapInputRecords(Job job) throws IOException {
    return job.getCounters()
        .getGroup("org.apache.hadoop.mapreduce.TaskCounter")
        .findCounter("MAP_INPUT_RECORDS")
        .getValue();
  }

  @Test
  public void testRunLocalMapReduce()
      throws IOException, ClassNotFoundException, InterruptedException {
    List<String> options = new ArrayList<>();
    ClientConfig clientConfig = new ClientConfig(ConfigUtils.defaultProperties());
    // database url
    options.add("-du");
    options.add(clientConfig.getDatabaseUrl());
    // database
    options.add("-dn");
    options.add(DATABASE_NAME);
    // table
    options.add("-t");
    options.add(TABLE_NAME);
    // user
    options.add("-u");
    options.add(clientConfig.getUsername());
    // password
    options.add("-p");
    options.add(clientConfig.getPassword());
    // fields
    for (int i = 1; i <= 29; i++) {
      options.add("-f");
      options.add("c" + i);
    }
    Job job = TiDBMapreduceDemo.createJob(options.toArray(new String[0]));
    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(1, getMapInputRecords(job));
  }

  @Test
  public void testMultipleRegions() throws Exception {
    String tableName = "test_multiple_regions";
    String dbTable = String.format("`%s`.`%s`", DATABASE_NAME, tableName);
    try (ClientSession clientSession = getSingleConnection()) {
      clientSession.sqlUpdate(
          "DROP TABLE IF EXISTS " + dbTable,
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (`id` INT PRIMARY KEY AUTO_INCREMENT)", dbTable),
          String.format(
              "INSERT INTO %s VALUES %s",
              dbTable, StringUtils.join(",", Collections.nCopies(10000, "(null)"))),
          String.format(
              "SPLIT TABLE `%s`.`%s` BETWEEN (0) AND (10000) REGIONS 10",
              DATABASE_NAME, tableName));
    }
    List<String> options = new ArrayList<>();
    ClientConfig clientConfig = new ClientConfig(ConfigUtils.defaultProperties());
    // database url
    options.add("-du");
    options.add(clientConfig.getDatabaseUrl());
    // database
    options.add("-dn");
    options.add(DATABASE_NAME);
    // table
    options.add("-t");
    options.add(tableName);
    // user
    options.add("-u");
    options.add(clientConfig.getUsername());
    // password
    options.add("-p");
    options.add(clientConfig.getPassword());
    // fields
    options.add("-f");
    options.add("id");

    Job job = TiDBMapreduceDemo.createJob(options.toArray(new String[0]));
    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(10000, getMapInputRecords(job));
  }

  @Test
  public void testEmptyRegions() throws Exception {
    String tableName = "test_empty_regions";
    String dbTable = String.format("`%s`.`%s`", DATABASE_NAME, tableName);
    try (ClientSession clientSession = getSingleConnection()) {
      clientSession.sqlUpdate(
          "DROP TABLE IF EXISTS " + dbTable,
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (`id` INT PRIMARY KEY AUTO_INCREMENT)", dbTable));
    }
    List<String> options = new ArrayList<>();
    ClientConfig clientConfig = new ClientConfig(ConfigUtils.defaultProperties());
    // database url
    options.add("-du");
    options.add(clientConfig.getDatabaseUrl());
    // database
    options.add("-dn");
    options.add(DATABASE_NAME);
    // table
    options.add("-t");
    options.add(tableName);
    // user
    options.add("-u");
    options.add(clientConfig.getUsername());
    // password
    options.add("-p");
    options.add(clientConfig.getPassword());
    // fields
    options.add("-f");
    options.add("id");

    Job job = TiDBMapreduceDemo.createJob(options.toArray(new String[0]));
    Assert.assertTrue(job.waitForCompletion(true));
    Assert.assertEquals(0, getMapInputRecords(job));
  }
}
