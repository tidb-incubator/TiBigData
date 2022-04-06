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
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.expression.Expression;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType;
import org.tikv.common.types.StringType;

@Category(IntegrationTest.class)
public class FilterPushDownLikeTest extends FilterPushDownTestBase {

  /**
   * Filters shot will return correct rows, and filters missed will return empty row list.
   */
  @Test
  public void testSupportedFilter() {
    List<Row> rows = validator.rows();

    // char
    String column = "c6";
    Object value = "chartype%";
    DataType type = validator.getColumnType(column);
    Expression expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "chartype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // varchar
    column = "c7";
    value = "varchartype%";
    type = validator.getColumnType(column);
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "varchartype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // tinytext
    column = "c8";
    value = "tinytexttype%";
    type = validator.getColumnType(column);
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "tinytexttype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // mediumtext
    column = "c9";
    value = "mediumtexttype%";
    type = validator.getColumnType(column);
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "mediumtexttype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // text
    column = "c10";
    value = "texttype%";
    type = validator.getColumnType(column);
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "texttype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // longtext
    column = "c11";
    value = "longtexttype%";
    type = validator.getColumnType(column);
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
    value = "longtexttype1";
    expression = Expressions.like(Expressions.column(column, type),
        Expressions.constant(value, type));
    validator.doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` like '%s'", column, value));

    // enum pushDown, only supported when TiKV version >= 5.1.0
    if (validator.getFilterPushDownHelper().isSupportEnumPushDown()) {
      column = "c28";
      value = "1%";
      type = StringType.VARCHAR;
      expression = Expressions.like(Expressions.column(column, type),
          Expressions.constant(value, type));
      validator.doTestFilter(rows, expression, String.format("`%s` like '%s'", column, value));
      value = "2";
      expression = Expressions.like(Expressions.column(column, type),
          Expressions.constant(value, type));
      validator.doTestFilter(ImmutableList.of(), expression,
          String.format("`%s` like '%s'", column, value));
    }
  }

  /**
   * Expression for not supported filter will be null and return all rows.
   */
  @Test
  public void testNotSupportedFilter() {
    List<Row> rows = validator.rows();
    // binary type is not supported, we do not test it.

    // json
    String column = "c27";
    Object value = "{\"a\": 1, \"b\": 2}";
    validator.doTestFilter(rows, null, String.format("`%s` like '%s'", column, value));

    // set
    column = "c29";
    value = "a";
    validator.doTestFilter(rows, null, String.format("`%s` like '%s'", column, value));

    // columns to columns
    validator.doTestFilter(rows, null, "`c11` like `c11`");
  }

}
