package io.tidb.bigdata.flink.tidb.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.test.ConfigUtils.defaultProperties;
import static java.lang.String.format;

import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

public class RealTimeWideTableTest extends FlinkTestBase {

  private String dstTable;

  protected static final String TABLE_SCHEMA =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  bigint,\n"
          + "    c2  bigint,\n"
          + "    c3  bigint,\n"
          + "    c4  bigint,\n"
          + "    unique key(c1)\n"
          + ")";

  @Test
  public void testInsertOnDuplicate() throws Exception {
    dstTable = RandomUtils.randomString();

    TableEnvironment tableEnvironment = getTableEnvironment();

    Map<String, String> properties = defaultProperties();
    properties.put(WRITE_MODE.key(), TiDBWriteMode.UPSERT.name());
    TiDBCatalog tiDBCatalog =
        initTiDBCatalog(dstTable, TABLE_SCHEMA, tableEnvironment, properties);

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
    tableEnvironment.sqlUpdate(sql2);
    tableEnvironment.sqlUpdate(sql3);
    tableEnvironment.execute("test");

    checkRowResult(tableEnvironment, Lists.newArrayList("+I[1, 7, 9, 32]"));
    Assert.assertEquals(1, tiDBCatalog.queryTableCount(DATABASE_NAME, dstTable));
  }

  private void checkRowResult(TableEnvironment tableEnvironment, List<String> expected) {
    Table table =
        tableEnvironment.sqlQuery(
            String.format("SELECT * FROM `tidb`.`%s`.`%s`", DATABASE_NAME, dstTable));
    CloseableIterator<Row> resultIterator = table.execute().collect();
    List<String> actualResult = Lists.newArrayList(resultIterator).stream().map(Row::toString).collect(
        Collectors.toList());

    Assert.assertEquals(actualResult, expected);
  }
}