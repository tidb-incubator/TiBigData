/*
 * Copyright 2022 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.tidb.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SKIP_CHECK_UPDATE_COLUMNS;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class RealTimeWideTableTest extends FlinkTestBase {

  private final Supplier<TableEnvironment> tableEnvironmentSupplier;
  private final String mode;

  public RealTimeWideTableTest(Supplier<TableEnvironment> tableEnvironmentSupplier, String mode) {
    this.tableEnvironmentSupplier = tableEnvironmentSupplier;
    this.mode = mode;
  }

  @Parameters(name = "{index}: mode={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          //          {(Supplier<TableEnvironment>) FlinkTestBase::getBatchTableEnvironment,
          // "Batch"},
          {(Supplier<TableEnvironment>) FlinkTestBase::getStreamingTableEnvironment, "Streaming"},
        });
  }

  private String dstTable;

  private static final String TABLE_SCHEMA =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint not null,\n"
          + "    c2  bigint,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    unique key(c1)\n"
          + ")";

  private static final String TABLE_SCHEMA_WITH_TWO_KEY =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint not null,\n"
          + "    c2  bigint not null,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    unique key(c1),\n"
          + "    primary key(c2)\n"
          + ")";

  private static final String TABLE_SCHEMA_WITH_MULTI_COLUMN_KEY =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint not null,\n"
          + "    c2  bigint not null,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    unique key(c1, c2)\n"
          + ")";

  private static final String TABLE_SCHEMA_WITH_TWO_MULTI_COLUMN_KEY =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint not null,\n"
          + "    c2  bigint,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    unique key(c1, c2),\n"
          + "    primary key(c1, c3)\n"
          + ")";

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @After
  public void teardown() {
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, dstTable));
  }

  @Test
  public void testInsertOnDuplicate() throws Exception {
    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    TiDBCatalog tiDBCatalog = initTiDBCatalog(dstTable, TABLE_SCHEMA, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    String sql2 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c3') */"
                + "values (1,28,9,4)",
            DATABASE_NAME, dstTable);
    String sql3 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2') */"
                + "values (1,7,5,46)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);
    System.out.println(sql2);
    System.out.println(sql3);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(sql2);
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(sql3);
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, 7, 9, 32]"));
    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  @Test
  public void testInsertOnDuplicateWithAppendMode() throws Exception {
    exceptionRule.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message", containsString("Insert on duplicate only work in `upsert` mode."))));

    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.APPEND.name());

    initTiDBCatalog(dstTable, TABLE_SCHEMA, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");
  }

  @Test
  public void testInsertOnDuplicateWithCatalogProperties() throws Exception {
    exceptionRule.expect(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString("Option tidb.sink.update-columns is only working for sql hint."))));
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    properties.put("tidb.sink.update-columns", "c1, c18");

    initTiDBCatalog(dstTable, TABLE_SCHEMA, tableEnvironment, properties);
  }

  @Test
  public void testInsertOnDuplicateWithMultiColumnIndex() throws Exception {
    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_MULTI_COLUMN_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    String sql2 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c3') */"
                + "values (1,2,356,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);
    System.out.println(sql2);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(sql2);
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, 2, 356, 32]"));
  }

  @Test
  public void testInsertOnDuplicateWithMoreThanOneKeyFields() throws Exception {
    exceptionRule.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString(
                    "Sink table should only have one unique key or primary key\n"
                        + "If you want to force skip the constraint, "
                        + "set `tidb.sink.skip-check-update-columns` to true"))));

    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_TWO_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");
  }

  @Test
  public void testInsertOnDuplicateWithMoreThanOneKeyFieldsSkipCheck() throws Exception {
    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    properties.put(SKIP_CHECK_UPDATE_COLUMNS.key(), "true");

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_TWO_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    String sql2 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c3') */"
                + "values (1,2,356,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);
    System.out.println(sql2);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(sql2);
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, 2, 356, 32]"));
  }

  @Test
  public void testInsertOnDuplicateWithTwoMultiColumnKey() throws Exception {
    exceptionRule.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString(
                    "Sink table should only have one unique key or primary key\n"
                        + "If you want to force skip the constraint, "
                        + "set `tidb.sink.skip-check-update-columns` to true"))));

    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_TWO_MULTI_COLUMN_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");
  }

  @Test
  public void testInsertOnDuplicateWithTwoMultiColumnIndexSkipCheck() throws Exception {
    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    properties.put(SKIP_CHECK_UPDATE_COLUMNS.key(), "true");

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_TWO_MULTI_COLUMN_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c3, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    String sql2 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c2, c3') */"
                + "values (1,2,356,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);
    System.out.println(sql2);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");

    tableEnvironment.sqlUpdate(sql2);
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, 2, 356, 32]"));
  }

  @Test
  public void testInsertOnDuplicateLackUpdateColumns() throws Exception {
    exceptionRule.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString(
                    "Update columns should contains all unique key columns or primary key columns\n"
                        + "If you want to force skip the constraint, "
                        + "set `tidb.sink.skip-check-update-columns` to true"))));

    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = tableEnvironmentSupplier.get();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());

    initTiDBCatalog(dstTable, TABLE_SCHEMA_WITH_MULTI_COLUMN_KEY, tableEnvironment, properties);

    String sql1 =
        format(
            "INSERT INTO `tidb`.`%s`.`%s` /*+ OPTIONS('tidb.sink.update-columns'='c1, c3, c4') */"
                + "values (1,2,3,32)",
            DATABASE_NAME, dstTable);
    System.out.println(sql1);

    tableEnvironment.sqlUpdate(sql1);
    tableEnvironment.execute("test");
  }

  private void checkRowResult(TableEnvironment tableEnvironment, List<String> expected) {
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    CloseableIterator<Row> resultIterator = table.execute().collect();
    List<String> actualResult =
        Lists.newArrayList(resultIterator).stream().map(Row::toString).collect(Collectors.toList());

    Assert.assertEquals(expected, actualResult);
  }
}
