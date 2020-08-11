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

package com.zhihu.tibigdata.tidb;

import static com.google.common.base.Preconditions.checkState;
import static com.pingcap.tikv.types.IntegerType.TINYINT;

import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.IsNull;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Not;
import com.pingcap.tikv.expression.StringRegExpression;
import com.pingcap.tikv.types.DataType;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public final class Expressions {

  private static final Expression CONSTANT_0 = new Constant(0, TINYINT);
  private static final Expression CONSTANT_1 = new Constant(1, TINYINT);

  private static final Expression ALWAYS_FALSE = equal(CONSTANT_0, CONSTANT_1);
  private static final Expression ALWAYS_TRUE = equal(CONSTANT_1, CONSTANT_1);

  public static Expression alwaysFalse() {
    return ALWAYS_FALSE;
  }

  public static Expression alwaysTrue() {
    return ALWAYS_TRUE;
  }

  public static Expression constant(Object value, DataType type) {
    return new Constant(value, type);
  }

  public static Expression column(String name) {
    return new ColumnRef(name);
  }

  public static Expression equal(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.equal(lhs, rhs);
  }

  public static Expression notEqual(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.notEqual(lhs, rhs);
  }

  public static Expression lessThan(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.lessThan(lhs, rhs);
  }

  public static Expression lessEqual(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.lessEqual(lhs, rhs);
  }

  public static Expression greaterThan(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.greaterThan(lhs, rhs);
  }

  public static Expression greaterEqual(Expression lhs, Expression rhs) {
    return ComparisonBinaryExpression.greaterEqual(lhs, rhs);
  }

  public static Expression plus(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.plus(lhs, rhs);
  }

  public static Expression minus(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.minus(lhs, rhs);
  }

  public static Expression multiply(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.multiply(lhs, rhs);
  }

  public static Expression divide(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.divide(lhs, rhs);
  }

  public static Expression bitAnd(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.bitAnd(lhs, rhs);
  }

  public static Expression bitOr(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.bitOr(lhs, rhs);
  }

  public static Expression bitXor(Expression lhs, Expression rhs) {
    return ArithmeticBinaryExpression.bitXor(lhs, rhs);
  }

  public static Expression isNull(Expression exp) {
    return new IsNull(exp);
  }

  public static Expression not(Expression exp) {
    return new Not(exp);
  }

  public static Expression and(Expression lhs, Expression rhs) {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    return LogicalBinaryExpression.and(lhs, rhs);
  }

  public static Expression and(Stream<Expression> expressions) {
    return expressions.reduce(null, Expressions::and);
  }

  public static Expression and(Collection<Expression> expressions) {
    return and(expressions.stream());
  }

  public static Optional<Expression> and(Optional<Expression> lhs, Optional<Expression> rhs) {
    return Optional.ofNullable(and(lhs.orElse(null), rhs.orElse(null)));
  }

  public static Expression or(Expression lhs, Expression rhs) {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    return LogicalBinaryExpression.or(lhs, rhs);
  }

  public static Expression or(Stream<Expression> expressions) {
    return expressions.reduce(null, Expressions::or);
  }

  public static Expression or(Collection<Expression> expressions) {
    return or(expressions.stream());
  }

  public static Expression in(Collection<Expression> expressions) {
    checkState(expressions.size() > 1);
    Stream<Expression> stream = expressions.stream();
    ColumnRef column = (ColumnRef) stream.findFirst().get();
    return or(stream.skip(1).map(expression -> equal(column, expression)));
  }

  public static Expression xor(Expression lhs, Expression rhs) {
    return LogicalBinaryExpression.xor(lhs, rhs);
  }

  public static Expression startsWith(Expression lhs, Expression rhs) {
    return StringRegExpression.startsWith(lhs, rhs);
  }

  public static Expression contains(Expression lhs, Expression rhs) {
    return StringRegExpression.contains(lhs, rhs);
  }

  public static Expression endsWith(Expression lhs, Expression rhs) {
    return StringRegExpression.endsWith(lhs, rhs);
  }

  public static Expression like(Expression lhs, Expression rhs) {
    return StringRegExpression.like(lhs, rhs);
  }

  public static String serialize(Expression expression) {
    return Serialization.serialize(expression);
  }

  public static Expression deserialize(String base64) {
    return Serialization.deserialize(base64);
  }
}