/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.prestodb.tidb.optimization;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;

import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.tikv.common.expression.ArithmeticBinaryExpression;
import org.tikv.common.expression.ComparisonBinaryExpression;
import org.tikv.common.expression.Expression;
import org.tikv.common.expression.Not;


public final class OperatorTranslators {

  private OperatorTranslators() {
  }

  @ScalarOperator(ADD)
  @SqlType(StandardTypes.BIGINT)
  public static Expression add(@SqlType(StandardTypes.BIGINT) Expression left,
      @SqlType(StandardTypes.BIGINT) Expression right) {
    return ArithmeticBinaryExpression.plus(left, right);
  }

  @ScalarOperator(SUBTRACT)
  @SqlType(StandardTypes.BIGINT)
  public static Expression subtract(@SqlType(StandardTypes.BIGINT) Expression left,
      @SqlType(StandardTypes.BIGINT) Expression right) {
    return ArithmeticBinaryExpression.minus(left, right);
  }

  @ScalarOperator(EQUAL)
  @SqlType(StandardTypes.BOOLEAN)
  public static Expression varcharEqual(@SqlType(StandardTypes.VARCHAR) Expression left,
      @SqlType(StandardTypes.VARCHAR) Expression right) {
    return ComparisonBinaryExpression.equal(left, right);
  }

  @ScalarOperator(EQUAL)
  @SqlType(StandardTypes.BOOLEAN)
  public static Expression bigintEqual(@SqlType(StandardTypes.BIGINT) Expression left,
      @SqlType(StandardTypes.BIGINT) Expression right) {
    return ComparisonBinaryExpression.equal(left, right);
  }

  @ScalarOperator(NOT_EQUAL)
  @SqlType(StandardTypes.BOOLEAN)
  public static Expression bigintNotEqual(@SqlType(StandardTypes.VARCHAR) Expression left,
      @SqlType(StandardTypes.VARCHAR) Expression right) {
    return ComparisonBinaryExpression.notEqual(left, right);
  }

  @ScalarOperator(NOT_EQUAL)
  @SqlType(StandardTypes.BOOLEAN)
  public static Expression varcharNotEqual(@SqlType(StandardTypes.BIGINT) Expression left,
      @SqlType(StandardTypes.BIGINT) Expression right) {
    return ComparisonBinaryExpression.notEqual(left, right);
  }

  @ScalarFunction("not")
  @SqlType(StandardTypes.BOOLEAN)
  public static Expression not(@SqlType(StandardTypes.BOOLEAN) Expression expression) {
    return new Not(expression);
  }
}

