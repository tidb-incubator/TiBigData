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

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.Expressions;
import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class FilterPushDownNullTest extends FilterPushDownTestBase {

  /** Filters shot will return correct rows, and filters missed will return empty row list. */
  @Test
  public void testSupportedFilter() {
    List<Row> rows = validator.rows();

    // IS NULL
    String column = "c1";
    DataType type = validator.getColumnType(column);
    Expression expression = Expressions.isNull(Expressions.column(column, type));
    validator.doTestFilter(ImmutableList.of(), expression, String.format("`%s` IS NULL", column));

    // NOT NULL
    expression = Expressions.not(Expressions.isNull(Expressions.column(column, type)));
    validator.doTestFilter(rows, expression, String.format("`%s` IS NOT NULL", column));
  }
}
