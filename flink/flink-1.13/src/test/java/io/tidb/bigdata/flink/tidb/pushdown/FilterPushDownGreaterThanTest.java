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
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.expression.Expression;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType;
import org.tikv.common.types.StringType;

@Category(IntegrationTest.class)
public class FilterPushDownGreaterThanTest {

  /**
   * Filters shot will return correct rows, and filters missed will return empty row list.
   */
  @Test
  public void testSupportedFilter() {
    List<Row> rows = rows();
    // tinyint
    String column = "c1";
    Object value = 0;
    DataType type = getColumnType(column);
    Expression expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > %s", column, value));
    value = 1;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > %s", column, value));
    // Swap column and value, we only test for tinyint, other types are same as tinyint
    value = 2;
    expression = Expressions.greaterThan(Expressions.constant(value, type),
        Expressions.column(column, type));
    doTestFilter(rows, expression, String.format("%s > `%s`", value, column));
    value = 1;
    expression = Expressions.greaterThan(Expressions.constant(value, type),
        Expressions.column(column, type));
    doTestFilter(ImmutableList.of(), expression, String.format("%s > `%s`", value, column));

    // smallint
    column = "c2";
    value = 0;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > %s", column, value));
    value = 1;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > %s", column, value));

    // mediumint
    column = "c3";
    value = 0;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > %s", column, value));
    value = 1;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > %s", column, value));

    // int
    column = "c4";
    value = 0;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > %s", column, value));
    value = 1;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > %s", column, value));

    // bigint
    column = "c5";
    value = 0;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > %s", column, value));
    value = 1;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > %s", column, value));

    // char
    column = "c6";
    value = "chartyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "chartype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // varchar
    column = "c7";
    value = "varchartyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "varchartype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // tinytext
    column = "c8";
    value = "tinytexttyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "tinytexttype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // mediumtext
    column = "c9";
    value = "mediumtexttyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "mediumtexttype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // text
    column = "c10";
    value = "texttyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "texttype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // longtext
    column = "c11";
    value = "longtexttyp";
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > '%s'", column, value));
    value = "longtexttype";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression, String.format("`%s` > '%s'", column, value));

    // float
    column = "c18";
    value = 1.233;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > CAST(%s AS FLOAT)", column, value));
    value = 1.234;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST(%s AS FLOAT)", column, value));

    // double
    column = "c19";
    value = 2.456788;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > CAST(%s AS DOUBLE)", column, value));
    value = 2.456789;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST(%s AS DOUBLE)", column, value));

    // decimal
    column = "c20";
    value = 123.455;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > CAST(%s AS DECIMAL(6,3))", column, value));
    value = 123.456;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST(%s AS DECIMAL(6,3))", column, value));

    // date
    column = "c21";
    value = Date.valueOf("2020-08-09");
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression, String.format("`%s` > CAST('%s' AS DATE)", column, value));
    value = Date.valueOf("2020-08-10");
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST('%s' AS DATE)", column, value));

    // datetime
    column = "c23";
    value = Timestamp.valueOf("2020-08-10 15:30:28");
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression,
        String.format("`%s` > CAST('%s' AS TIMESTAMP(6))", column, value));
    value = Timestamp.valueOf("2020-08-10 15:30:29");
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST('%s' AS TIMESTAMP(6))", column, value));

    // timestamp
    column = "c24";
    Timestamp timestamp = Timestamp.valueOf("2020-08-10 16:30:28");
    ZonedDateTime zonedDateTime = ZonedDateTime.of(timestamp.toLocalDateTime(),
        ZoneId.systemDefault());
    ZonedDateTime utc = zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
    Timestamp utcTimestamp = Timestamp.valueOf(utc.toLocalDateTime());
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(utcTimestamp, type));
    doTestFilter(rows, expression,
        String.format("`%s` > CAST('%s' AS TIMESTAMP(6))", column, timestamp));

    timestamp = Timestamp.valueOf("2020-08-10 16:30:29");
    zonedDateTime = ZonedDateTime.of(timestamp.toLocalDateTime(),
        ZoneId.systemDefault());
    utc = zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
    utcTimestamp = Timestamp.valueOf(utc.toLocalDateTime());
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(utcTimestamp, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > CAST('%s' AS TIMESTAMP(6))", column, timestamp));

    // year
    column = "c25";
    value = 2019;
    type = getColumnType(column);
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression,
        String.format("`%s` > %s", column, value));
    value = 2020;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > %s", column, value));

    // enum
    column = "c28";
    value = "0";
    type = StringType.VARCHAR;
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(rows, expression,
        String.format("`%s` > '%s'", column, value));
    value = "1";
    expression = Expressions.greaterThan(Expressions.column(column, type),
        Expressions.constant(value, type));
    doTestFilter(ImmutableList.of(), expression,
        String.format("`%s` > '%s'", column, value));

  }

  /**
   * Expression for not supported filter will be null and return all rows.
   */
  @Test
  public void testNotSupportedFilter() {
    List<Row> rows = rows();
    // binary type is not supported, we do not test it.

    // json
    String column = "c27";
    Object value = "{\"a\": 1, \"b\": 2}";
    doTestFilter(rows, null, String.format("`%s` > '%s'", column, value));

    // set
    column = "c29";
    value = "a";
    doTestFilter(rows, null, String.format("`%s` > '%s'", column, value));

    // columns to columns
    doTestFilter(rows, null, "`c1` > `c2`");
  }

}
