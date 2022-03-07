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

package io.tidb.bigdata.flink.tidb;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.flink.connector.TiDBCatalog;
import io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.TableUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType;

@Category(IntegrationTest.class)
public class FilterPushDownValidator {

  private static final String DATABASE = "test";
  private static final String TABLE = "all_types";
  private static final String INSERT_SQL = String.format("INSERT IGNORE INTO `%s`.`%s`\n"
      + "VALUES (\n"
      + " 1,\n"
      + " 1,\n"
      + " 1,\n"
      + " 1,\n"
      + " 1,\n"
      + " 'chartype',\n"
      + " 'varchartype',\n"
      + " 'tinytexttype',\n"
      + " 'mediumtexttype',\n"
      + " 'texttype',\n"
      + " 'longtexttype',\n"
      + " 'binarytype',\n"
      + " 'varbinarytype',\n"
      + " 'tinyblobtype',\n"
      + " 'mediumblobtype',\n"
      + " 'blobtype',\n"
      + " 'longblobtype',\n"
      + " 1.234,\n"
      + " 2.456789,\n"
      + " 123.456,\n"
      + " '2020-08-10',\n"
      + " '15:30:29',\n"
      + " '2020-08-10 15:30:29',\n"
      + " '2020-08-10 16:30:29',\n"
      + " 2020,\n"
      + " true,\n"
      + " '{\"a\":1,\"b\":2}',\n"
      + " '1',\n"
      + " 'a'\n"
      + ")", DATABASE, TABLE);

  private static final FilterPushDownValidator instance = new FilterPushDownValidator();

  private TiDBCatalog catalog;
  private ClientSession clientSession;
  private TiTableInfo tiTableInfo;
  private List<Row> rows;
  private Map<String, DataType> nameTypeMap;
  private FilterPushDownHelper filterPushDownHelper;

  private FilterPushDownValidator() {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    this.catalog = new TiDBCatalog("tidb", properties);
    this.clientSession = ClientSession.create(new ClientConfig(properties));
    catalog.open();
    catalog.sqlUpdate(TableUtils.getTableSqlWithAllTypes(DATABASE, TABLE), INSERT_SQL);
    this.tiTableInfo = clientSession.getTableMust(DATABASE, TABLE);
    this.rows = ImmutableList.copyOf(scanRows(DATABASE, TABLE, Optional.empty()));
    this.nameTypeMap = tiTableInfo.getColumns().stream()
        .collect(Collectors.toMap(TiColumnInfo::getName, TiColumnInfo::getType));
    this.filterPushDownHelper = new FilterPushDownHelper(tiTableInfo);
  }

  private List<Row> scanRows(String database, String table, Optional<Expression> expression) {
    List<Row> rows = new ArrayList<>();
    List<SplitInternal> splits = new SplitManagerInternal(clientSession).getSplits(
        new TableHandleInternal("", database, table));
    List<ColumnHandleInternal> columns = clientSession.getTableColumnsMust(database, table);
    for (SplitInternal split : splits) {
      RecordSetInternal recordSetInternal = new RecordSetInternal(clientSession, split, columns,
          expression, Optional.empty());
      RecordCursorInternal cursor = recordSetInternal.cursor();
      while (cursor.advanceNextPosition()) {
        rows.add(cursor.getRow());
      }
    }
    return rows;
  }

  private static Object[] toObjectArray(Row row) {
    Object[] objects = new Object[row.fieldCount()];
    for (int i = 0; i < row.fieldCount(); i++) {
      objects[i] = row.get(i, null);
    }
    return objects;
  }

  /**
   * Test for expressions and rows.
   */
  public static void doTestFilter(List<Row> expectedRows, Expression expectedExpression,
      String whereCondition) {
    List<ResolvedExpression> filters = FilterPushDownTestUtils.getFilters(whereCondition);
    Expression actualExpression = instance.filterPushDownHelper.toTiDBExpression(filters)
        .orElse(null);
    Assert.assertEquals(Objects.toString(expectedExpression), Objects.toString(actualExpression));
    List<Row> rows = instance.scanRows(DATABASE, TABLE, Optional.ofNullable(actualExpression));
    Assert.assertEquals(expectedRows.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Object[] expected = toObjectArray(expectedRows.get(i));
      Object[] actual = toObjectArray(rows.get(i));
      Assert.assertArrayEquals(expected, actual);
    }
  }

  public static DataType getColumnType(String column) {
    return instance.nameTypeMap.get(column);
  }

  public static List<Row> rows() {
    return instance.rows;
  }


}
