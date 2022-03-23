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

package io.tidb.bigdata.flink.tidb.pushdown;

import static io.tidb.bigdata.flink.tidb.pushdown.FilterPushDownValidator.doTestFilter;
import static io.tidb.bigdata.flink.tidb.pushdown.FilterPushDownValidator.getColumnType;
import static io.tidb.bigdata.flink.tidb.pushdown.FilterPushDownValidator.rows;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.Expressions;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.expression.Expression;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType;

@Category(IntegrationTest.class)
public class FilterPushDownNullTest {

  /**
   * Filters shot will return correct rows, and filters missed will return empty row list.
   */
  @Test
  public void testSupportedFilter() {
    List<Row> rows = rows();

    // IS NULL
    String column = "c1";
    DataType type = getColumnType(column);
    Expression expression = Expressions.isNull(Expressions.column(column, type));
   doTestFilter(ImmutableList.of(), expression, String.format("`%s` IS NULL", column));

    // NOT NULL
    expression = Expressions.not(Expressions.isNull(Expressions.column(column, type)));
   doTestFilter(rows, expression, String.format("`%s` IS NOT NULL", column));

  }

}
