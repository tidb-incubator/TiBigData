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

package io.tidb.bigdata.flink.tidb.partition;

import static io.tidb.bigdata.flink.connector.TiDBOptions.*;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.TIKV;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBOptions;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class PartitionWriteTest extends FlinkTestBase {

  private static final String DST_TABLE = "partition_write_test_table";

  @Test
  public void testHashPartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`id` BIGINT PRIMARY KEY, `name` VARCHAR(16)) PARTITION BY HASH(`id`) PARTITIONS 3",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES (5, 'Apple'), (25, 'Honey'), (29, 'Mike')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");
    String querySQL =
        String.format("SELECT * FROM `tidb`.`%s`.`%s` ORDER BY `id`", DATABASE_NAME, DST_TABLE);
    checkResult(
        tableEnvironment,
        Lists.newArrayList("+I[5, Apple]", "+I[25, Honey]", "+I[29, Mike]"),
        querySQL);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES (5, 'Origin'), (25, 'Bee')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");
    String querySQL2 =
        String.format("SELECT * FROM `tidb`.`%s`.`%s` ORDER BY `id`", DATABASE_NAME, DST_TABLE);
    checkResult(
        tableEnvironment,
        Lists.newArrayList("+I[5, Origin]", "+I[25, Bee]", "+I[29, Mike]"),
        querySQL2);
  }

  @Test
  public void testYearHashPartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`birthday` DATE PRIMARY KEY, `name` VARCHAR(16)) PARTITION BY HASH(YEAR(`birthday`)) PARTITIONS 4",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES (CAST('1995-06-15' AS DATE), 'Apple'), (CAST('1995-08-08' AS DATE), 'Honey'), (CAST('1999-06-04' AS DATE), 'Mike')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");
    String querySQL =
        String.format(
            "SELECT * FROM `tidb`.`%s`.`%s` ORDER BY `birthday`", DATABASE_NAME, DST_TABLE);
    checkResult(
        tableEnvironment,
        Lists.newArrayList(
            "+I[1995-06-15, Apple]", "+I[1995-08-08, Honey]", "+I[1999-06-04, Mike]"),
        querySQL);
  }

  @Test
  public void testRangePartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`id` BIGINT PRIMARY KEY, `name` VARCHAR(16)) PARTITION BY RANGE (`id`) (PARTITION p0 VALUES LESS THAN (20), PARTITION p1 VALUES LESS THAN (60), PARTITION p2 VALUES LESS THAN MAXVALUE)",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES (19, 'Mike'), (59, 'Apple'), (376, 'Jack')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect = new ArrayList<>();
    expect.add(Lists.newArrayList(19L, "Mike"));
    checkJDBCResult(querySQL, expect);

    String querySQL2 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect2 = new ArrayList<>();
    expect2.add(Lists.newArrayList(19L, "Mike"));
    expect2.add(Lists.newArrayList(376L, "Jack"));
    checkJDBCResult(querySQL2, expect2);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES (19, 'Marry'), (59, 'Origin'), (376, 'Tom')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL3 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect3 = new ArrayList<>();
    expect3.add(Lists.newArrayList(19L, "Marry"));
    checkJDBCResult(querySQL3, expect3);

    String querySQL4 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect4 = new ArrayList<>();
    expect4.add(Lists.newArrayList(59L, "Origin"));
    expect4.add(Lists.newArrayList(376L, "Tom"));
    checkJDBCResult(querySQL4, expect4);
  }

  @Test
  public void testDateTimePartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`birthday` DATETIME PRIMARY KEY, `name` VARCHAR(16)) "
                + "PARTITION BY RANGE COLUMNS (`birthday`) "
                + "(PARTITION p0 VALUES LESS THAN ('1995-07-17 15:15:15'),"
                + "PARTITION p1 VALUES LESS THAN ('1996-01-01 15:15:15'),"
                + "PARTITION p2 VALUES LESS THAN MAXVALUE)",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15 15:15:15' AS TIMESTAMP), 'Apple'), "
                + "(CAST('1995-08-08 15:15:15' AS TIMESTAMP), 'Honey'), "
                + "(CAST('1999-06-15 15:15:15' AS TIMESTAMP), 'Mike')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect = new ArrayList<>();
    expect.add(Lists.newArrayList(dateTimeToLocalDateTime("1995-06-15 15:15:15"), "Apple"));
    checkJDBCResult(querySQL, expect);

    String querySQL2 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect2 = new ArrayList<>();
    expect2.add(Lists.newArrayList(dateTimeToLocalDateTime("1995-08-08 15:15:15"), "Honey"));
    expect2.add(Lists.newArrayList(dateTimeToLocalDateTime("1999-06-15 15:15:15"), "Mike"));
    checkJDBCResult(querySQL2, expect2);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15 15:15:15' AS TIMESTAMP), 'Jack'), "
                + "(CAST('1995-08-08 15:15:15' AS TIMESTAMP), 'Mary'), "
                + "(CAST('1999-06-15 15:15:15' AS TIMESTAMP), 'John')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL3 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect3 = new ArrayList<>();
    expect3.add(Lists.newArrayList(dateTimeToLocalDateTime("1995-06-15 15:15:15"), "Jack"));
    checkJDBCResult(querySQL3, expect3);

    String querySQL4 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect4 = new ArrayList<>();
    expect4.add(Lists.newArrayList(dateTimeToLocalDateTime("1995-08-08 15:15:15"), "Mary"));
    expect4.add(Lists.newArrayList(dateTimeToLocalDateTime("1999-06-15 15:15:15"), "John"));
    checkJDBCResult(querySQL4, expect4);
  }

  @Test
  public void testDatePartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`birthday` DATE PRIMARY KEY, `name` VARCHAR(16)) "
                + "PARTITION BY RANGE COLUMNS (`birthday`) "
                + "(PARTITION p0 VALUES LESS THAN ('1995-07-17'),"
                + "PARTITION p1 VALUES LESS THAN ('1996-01-01'),"
                + "PARTITION p2 VALUES LESS THAN MAXVALUE)",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15' AS DATE), 'Apple'), "
                + "(CAST('1995-08-08' AS DATE), 'Honey'), "
                + "(CAST('1999-06-15' AS DATE), 'Mike')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect = new ArrayList<>();
    expect.add(Lists.newArrayList(dateStringToDate("1995-06-15"), "Apple"));
    checkJDBCResult(querySQL, expect);

    String querySQL2 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect2 = new ArrayList<>();
    expect2.add(Lists.newArrayList(dateStringToDate("1995-08-08"), "Honey"));
    expect2.add(Lists.newArrayList(dateStringToDate("1999-06-15"), "Mike"));
    checkJDBCResult(querySQL2, expect2);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15' AS DATE), 'Jack'), "
                + "(CAST('1995-08-08' AS DATE), 'Mary'), "
                + "(CAST('1999-06-15' AS DATE), 'John')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL3 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect3 = new ArrayList<>();
    expect3.add(Lists.newArrayList(dateStringToDate("1995-06-15"), "Jack"));
    checkJDBCResult(querySQL3, expect3);

    String querySQL4 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect4 = new ArrayList<>();
    expect4.add(Lists.newArrayList(dateStringToDate("1995-08-08"), "Mary"));
    expect4.add(Lists.newArrayList(dateStringToDate("1999-06-15"), "John"));
    checkJDBCResult(querySQL4, expect4);
  }

  @Test
  public void testBinaryRangeColumnPartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`id` varbinary(16) PRIMARY KEY, nums int(3)) "
                + "PARTITION BY RANGE COLUMNS (`id`) "
                + "(PARTITION p0 VALUES LESS THAN (X'424242424242'),"
                + "PARTITION p1 VALUES LESS THAN (X'525252525252'),"
                + "PARTITION p2 VALUES LESS THAN MAXVALUE)",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('Apple' AS BYTES), 10), "
                + "(CAST('John' AS BYTES), 20), "
                + "(CAST('Mike' AS BYTES), 30)",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL =
        String.format("SELECT * FROM  `tidb`.`%s`.`%s` ORDER BY `nums`", DATABASE_NAME, DST_TABLE);
    checkResult(
        tableEnvironment,
        Lists.newArrayList(
            "+I[[65, 112, 112, 108, 101], 10]",
            "+I[[74, 111, 104, 110], 20]",
            "+I[[77, 105, 107, 101], 30]"),
        querySQL);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('Apple' AS BYTES), 40), "
                + "(CAST('John' AS BYTES), 50), "
                + "(CAST('Mike' AS BYTES), 60)",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");
    String querySQL2 =
        String.format("SELECT * FROM  `tidb`.`%s`.`%s` ORDER BY `nums`", DATABASE_NAME, DST_TABLE);
    checkResult(
        tableEnvironment,
        Lists.newArrayList(
            "+I[[65, 112, 112, 108, 101], 40]",
            "+I[[74, 111, 104, 110], 50]",
            "+I[[77, 105, 107, 101], 60]"),
        querySQL2);
  }

  @Test
  public void testYearRangePartition() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE `%s`.`%s` (`birthday` DATE PRIMARY KEY, `name` VARCHAR(16)) "
                + "PARTITION BY RANGE (YEAR(`birthday`)) "
                + "(PARTITION p0 VALUES LESS THAN (1996),"
                + "PARTITION p1 VALUES LESS THAN (YEAR('1997-01-01')),"
                + "PARTITION p2 VALUES LESS THAN MAXVALUE)",
            DATABASE_NAME, DST_TABLE);
    TableEnvironment tableEnvironment = genTableEnvironmentForPartitionTest(createTableSql);

    // insert
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15' AS DATE), 'Apple'), "
                + "(CAST('1996-08-08' AS DATE), 'Honey'), "
                + "(CAST('1999-06-15' AS DATE), 'Mike')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect = new ArrayList<>();
    expect.add(Lists.newArrayList(dateStringToDate("1995-06-15"), "Apple"));
    checkJDBCResult(querySQL, expect);

    String querySQL2 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect2 = new ArrayList<>();
    expect2.add(Lists.newArrayList(dateStringToDate("1996-08-08"), "Honey"));
    expect2.add(Lists.newArrayList(dateStringToDate("1999-06-15"), "Mike"));
    checkJDBCResult(querySQL2, expect2);

    // update
    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` VALUES "
                + "(CAST('1995-06-15' AS DATE), 'Jack'), "
                + "(CAST('1996-08-08' AS DATE), 'Mary'), "
                + "(CAST('1999-06-15' AS DATE), 'John')",
            DATABASE_NAME, DST_TABLE));
    tableEnvironment.execute("test");

    String querySQL3 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p0)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect3 = new ArrayList<>();
    expect3.add(Lists.newArrayList(dateStringToDate("1995-06-15"), "Jack"));
    checkJDBCResult(querySQL3, expect3);

    String querySQL4 =
        String.format("SELECT * FROM `%s`.`%s` PARTITION (p1, p2)", DATABASE_NAME, DST_TABLE);
    List<List<Object>> expect4 = new ArrayList<>();
    expect4.add(Lists.newArrayList(dateStringToDate("1996-08-08"), "Mary"));
    expect4.add(Lists.newArrayList(dateStringToDate("1999-06-15"), "John"));
    checkJDBCResult(querySQL4, expect4);
  }

  private LocalDateTime dateTimeToLocalDateTime(String date) {
    return LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  private Date dateStringToDate(String date) throws ParseException {
    return new SimpleDateFormat("yyyy-MM-dd").parse(date);
  }

  private TableEnvironment genTableEnvironmentForPartitionTest(String createTableSql) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = defaultProperties();
    properties.put(SINK_IMPL.key(), TIKV.name());
    properties.put(SINK_TRANSACTION.key(), TiDBOptions.SinkTransaction.GLOBAL.name());
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    initTiDBCatalog(DST_TABLE, createTableSql, tableEnvironment, properties);
    return tableEnvironment;
  }

  private void checkResult(TableEnvironment tableEnvironment, List<String> expected, String sql) {
    Table table = tableEnvironment.sqlQuery(sql);
    CloseableIterator<Row> resultIterator = table.execute().collect();
    List<String> actualResult =
        Lists.newArrayList(resultIterator).stream().map(Row::toString).collect(Collectors.toList());

    Assert.assertEquals(expected, actualResult);
  }

  private void checkJDBCResult(String sql, List<List<Object>> expect) throws Exception {
    try (ClientSession clientSession = ClientSession.create(new ClientConfig(defaultProperties()));
        Connection connection = clientSession.getJdbcConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      List<List<Object>> resultByJDBC = new ArrayList<>();
      while (resultSet.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= expect.get(0).size(); i++) {
          row.add(resultSet.getObject(i));
        }
        resultByJDBC.add(row);
      }
      Assert.assertTrue(CollectionUtils.isEqualCollection(expect, resultByJDBC));
    }
  }
}
