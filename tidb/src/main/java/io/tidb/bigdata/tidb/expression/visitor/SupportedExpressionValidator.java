/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.tidb.expression.visitor;

import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.expression.ExpressionBlocklist;

public class SupportedExpressionValidator extends DefaultVisitor<Boolean, ExpressionBlocklist> {
  private static final SupportedExpressionValidator validator = new SupportedExpressionValidator();

  public static boolean isSupportedExpression(Expression node, ExpressionBlocklist blocklist) {
    if (!node.accept(validator, blocklist)) {
      return false;
    }
    try {
      ProtoConverter protoConverter = new ProtoConverter(false);
      if (node.accept(protoConverter, null) == null) {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  protected Boolean process(Expression node, ExpressionBlocklist blocklist) {
    if (blocklist != null && blocklist.isUnsupportedPushDownExpr(getClass())) {
      return false;
    }
    for (Expression expr : node.getChildren()) {
      if (!expr.accept(this, blocklist)) {
        return false;
      }
    }
    return true;
  }
}
