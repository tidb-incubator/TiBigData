package io.tidb.bigdata.tidb;

import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.handle.TableHandleInternal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MemoryCoprocessorIteratorTest {

  @Test
  public void testRead() throws Exception {
    String databaseName = "test";
    String tableName = "test_multiple_regions";
    String dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put(ClientConfig.TIDB_STORED_ROWS_IN_MEMORY, "true");
    try (ClientSession session = ClientSession.create(new ClientConfig(properties))) {
      session.sqlUpdate(
          "DROP TABLE IF EXISTS " + dbTable,
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (`id` INT PRIMARY KEY AUTO_INCREMENT)", dbTable),
          String.format(
              "INSERT INTO %s VALUES %s",
              dbTable, String.join(",", Collections.nCopies(10000, "(null)"))),
          String.format(
              "SPLIT TABLE `%s`.`%s` BETWEEN (0) AND (10000) REGIONS 10", databaseName, tableName));
      SplitManagerInternal splitManagerInternal = new SplitManagerInternal(session);
      List<SplitInternal> splits =
          splitManagerInternal.getSplits(new TableHandleInternal("", databaseName, tableName));
      List<ColumnHandleInternal> columns = session.getTableColumnsMust(databaseName, tableName);
      Assert.assertEquals(10, splits.size());
      RecordSetInternal recordSetInternal =
          new RecordSetInternal(
              session, splits, columns, Optional.empty(), Optional.empty(), Optional.empty());
      List<Integer> rows = new ArrayList<>(10000);
      try (RecordCursorInternal cursor = recordSetInternal.cursor()) {
        while (cursor.advanceNextPosition()) {
          rows.add(cursor.getInteger(0));
        }
      }
      rows.sort(Comparator.naturalOrder());
      Assert.assertEquals(IntStream.range(1, 10001).boxed().collect(Collectors.toList()), rows);
    }
  }
}
