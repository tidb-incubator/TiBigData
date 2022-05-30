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

package io.tidb.bigdata.flink.tidb.clustered;

import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.catalog.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(org.junit.runners.Parameterized.class)
public class ClusteredIndexTest extends FlinkTestBase {

  private static final List<Pair<String, String>> dataTypesList =
      Lists.newArrayList(
          new Pair<>("bit(11)", "0"),
          new Pair<>("int", "11"),
          new Pair<>("decimal", "110"),
          new Pair<>("decimal(6,3)", "110.000"),
          new Pair<>("timestamp", "2020-08-10 15:30:29"),
          new Pair<>("text(11)", "textType"),
          new Pair<>("blob(11)", "blobType"),
          new Pair<>("boolean", "0"),
          new Pair<>("bigint", "1234"),
          new Pair<>("double", "12.3"),
          new Pair<>("date", "1995-01-01"),
          new Pair<>("varchar(20)", "varcharType"));

  private static final String TABLE_CLUSTERED_INDEX_ONE_COLUMN =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  %s,\n"
          + "    PRIMARY KEY (`c1`) /*T![clustered_index] CLUSTERED */\n"
          + ")";

  @Parameters(name = "{index}: dataType={0}")
  public static Collection<Object[]> data() {
    ArrayList<Object[]> parameters = Lists.newArrayList();
    for (Pair<String, String> dataType : dataTypesList) {
      parameters.add(new Object[] {dataType});
    }

    return parameters;
  }

  private final Pair<String, String> dataType;

  public ClusteredIndexTest(Pair<String, String> dataType) {
    this.dataType = dataType;
  }

  @BeforeClass
  public static void setUp() {
    org.junit.Assume.assumeTrue(testDatabase.getClientSession().supportClusteredIndex());
  }

  private String srcTable;

  private String dstTable;

  @Test
  public void testClusteredIndexWithOneColumn() throws Exception {
    srcTable = RandomUtils.randomString();
    dstTable = RandomUtils.randomString();
    TableEnvironment tableEnvironment = getTableEnvironment();

    Map<String, String> properties = defaultProperties();

    String createTableSql =
        String.format(TABLE_CLUSTERED_INDEX_ONE_COLUMN, "%s", "%s", dataType.getKey());
    if (dataType.getKey().equals("blob(11)") || dataType.getKey().equals("text(11)")) {
      createTableSql = createTableSql.replace("PRIMARY KEY (`c1`)", "PRIMARY KEY (`c1`(11))");
    }

    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, createTableSql, tableEnvironment, properties);

    tableEnvironment.sqlUpdate(
        String.format(
            "INSERT INTO `tidb`.`%s`.`%s` " + "VALUES (%s)",
            DATABASE_NAME, dstTable, castString(dataType.getKey(), dataType.getValue())));
    tableEnvironment.execute("test");

    checkSourceScanResult(tableEnvironment);

    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  private void checkSourceScanResult(TableEnvironment tableEnvironment) {
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    Row next = table.execute().collect().next();
    if (dataType.getKey().toLowerCase().startsWith("blob")) {
      Assert.assertArrayEquals(
          (byte[]) next.getField("c1"), dataType.getValue().getBytes(StandardCharsets.UTF_8));
    } else if (dataType.getKey().toLowerCase().startsWith("timestamp")) {
      Assert.assertEquals(
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
              .format((LocalDateTime) Objects.requireNonNull(next.getField("c1"))),
          dataType.getValue());
    } else {
      Assert.assertEquals(String.valueOf(next.getField("c1")), dataType.getValue());
    }
  }

  public static String castString(String type, String value) {
    if (type.toLowerCase().startsWith("boolean")) {
      return String.format("cast('%s' as TINYINT)", value);
    } else if (type.toLowerCase().startsWith("blob")) {
      return String.format("cast('%s' as bytes)", value);
    } else if (type.toLowerCase().startsWith("text")) {
      return String.format("cast('%s' as string)", value);
    } else if (type.toLowerCase().startsWith("timestamp")
        || type.toLowerCase().startsWith("date")
        || type.toLowerCase().startsWith("varchar")) {
      return String.format("cast('%s' as %s)", value, type);
    } else {
      return value;
    }
  }

  @After
  public void teardown() {
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, srcTable));
    testDatabase
        .getClientSession()
        .sqlUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`", DATABASE_NAME, dstTable));
  }
}
