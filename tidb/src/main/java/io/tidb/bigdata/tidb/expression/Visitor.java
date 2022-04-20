/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.expression;

public abstract class Visitor<R, C> {
  protected abstract R visit(ColumnRef node, C context);

  protected abstract R visit(ComparisonBinaryExpression node, C context);

  protected abstract R visit(StringRegExpression node, C context);

  protected abstract R visit(ArithmeticBinaryExpression node, C context);

  protected abstract R visit(LogicalBinaryExpression node, C context);

  protected abstract R visit(Constant node, C context);

  protected abstract R visit(AggregateFunction node, C context);

  protected abstract R visit(IsNull node, C context);

  protected abstract R visit(Not node, C context);

  protected abstract R visit(FuncCallExpr node, C context);
}
