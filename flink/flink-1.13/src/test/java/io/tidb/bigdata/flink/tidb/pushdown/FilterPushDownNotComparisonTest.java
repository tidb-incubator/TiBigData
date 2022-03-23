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

@Category(IntegrationTest.class)
public class FilterPushDownNotComparisonTest {

  Expression c1 = Expressions.column("c1", getColumnType("c1"));
  Expression c2 = Expressions.column("c2", getColumnType("c2"));
  Expression c6 = Expressions.column("c6", getColumnType("c6"));
  Expression constant_1 = Expressions.constant(1, null);
  Expression constant_2 = Expressions.constant(2, null);
  Expression constant_3 = Expressions.constant(3, null);
  Expression constant_string = Expressions.constant("chartype", null);
  Expression c1_equal_1 = Expressions.equal(c1, constant_1);
  Expression c1_equal_2 = Expressions.equal(c1, constant_2);
  Expression c1_equal_3 = Expressions.equal(c1, constant_3);
  Expression c2_equal_1 = Expressions.equal(c2, constant_1);
  Expression c2_equal_2 = Expressions.equal(c2, constant_2);
  Expression c6_equal_chartype = Expressions.equal(c6, constant_string);
  Expression c1_equal_c2 = Expressions.equal(c1, c2);

  /**
   * Filters shot will return correct rows, and filters missed will return empty row list.
   */
  @Test
  public void testSupportedFilter() {
    List<Row> rows = rows();
    // in
    String whereCondition = "`c1` IN (1,2)";
    Expression expression = Expressions.or(c1_equal_1, c1_equal_2);
   doTestFilter(rows, expression, whereCondition);
    whereCondition = "`c1` IN (2,3)";
    expression = Expressions.or(c1_equal_2, c1_equal_3);
   doTestFilter(ImmutableList.of(), expression, whereCondition);

    // or
    whereCondition = "`c1` = 1 or c1 = 2";
    expression = Expressions.or(c1_equal_1, c1_equal_2);
   doTestFilter(rows, expression, whereCondition);

    whereCondition = "`c1` = 2 or c1 = 3";
    expression = Expressions.or(c1_equal_2, c1_equal_3);
   doTestFilter(ImmutableList.of(), expression, whereCondition);

    // and
    whereCondition = "`c1` = 1 AND `c2` = 1 AND `c6` = 'chartype'";
    expression = Expressions.and(ImmutableList.of(c1_equal_1, c2_equal_1, c6_equal_chartype));
   doTestFilter(rows, expression, whereCondition);

    whereCondition = "`c1` = 1 AND `c2` = 2 AND `c6` = 'chartype'";
    expression = Expressions.and(ImmutableList.of(c1_equal_1, c2_equal_2, c6_equal_chartype));
   doTestFilter(ImmutableList.of(), expression, whereCondition);

    // or & and
    whereCondition = "(`c1` = 2 AND `c2` = 2) OR `c1` = 1";
    expression = Expressions.and(Expressions.or(c1_equal_2, c1_equal_1),
        Expressions.or(c2_equal_2, c1_equal_1));
   doTestFilter(rows, expression, whereCondition);

    whereCondition = "(`c1` = 2 AND `c2` = 2) OR `c1` = 3";
    expression = Expressions.and(Expressions.or(c1_equal_2, c1_equal_3),
        Expressions.or(c2_equal_2, c1_equal_3));
   doTestFilter(ImmutableList.of(), expression, whereCondition);

  }

  /**
   * Expression for not supported filter will be null and return all rows.
   */
  @Test
  public void testNotSupportedFilter() {
    List<Row> rows = rows();
    // or
    String whereCondition = "`c1` = 1 or `c1` = `c2`";
   doTestFilter(rows, null, whereCondition);

    // and
    whereCondition = "`c1` = 1 AND `c2` = 1 AND `c1` = `c2`";
    Expression expression = Expressions.and(ImmutableList.of(c1_equal_1, c2_equal_1));
   doTestFilter(rows, expression, whereCondition);

    // and
    whereCondition = "`c1` = 2 AND `c2` = 1 AND `c1` = `c2`";
    expression = Expressions.and(ImmutableList.of(c1_equal_2, c2_equal_1));
   doTestFilter(ImmutableList.of(), expression, whereCondition);
  }

}
