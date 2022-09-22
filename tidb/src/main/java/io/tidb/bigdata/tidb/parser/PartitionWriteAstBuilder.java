/*
 * Copyright 2022 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.tidb.parser;

import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.expression.FuncCallExpr;
import io.tidb.bigdata.tidb.meta.TiTableInfo;

public class PartitionWriteAstBuilder extends AstBuilder {
  public PartitionWriteAstBuilder() {
    super();
  }

  public PartitionWriteAstBuilder(TiTableInfo tableInfo) {
    super(tableInfo);
  }

  @Override
  public Expression visitScalarFunctionCall(MySqlParser.ScalarFunctionCallContext ctx) {
    MySqlParser.FunctionNameBaseContext fnNameCtx = ctx.scalarFunctionName().functionNameBase();
    if (fnNameCtx != null) {
      if (fnNameCtx.YEAR() != null) {
        Expression args = visitFunctionArgs(ctx.functionArgs());
        return new FuncCallExpr(args, FuncCallExpr.Type.YEAR);
      } else {
        throw new UnsupportedOperationException("Unsupported function: " + fnNameCtx.getText());
      }
    }
    return visitChildren(ctx);
  }
}
