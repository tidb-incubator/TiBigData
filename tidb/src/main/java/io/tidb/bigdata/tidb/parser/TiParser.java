/*
 *
 * Copyright 2019 TiDB Project Authors.
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
 *
 */

package io.tidb.bigdata.tidb.parser;

import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.tikv.common.exception.UnsupportedSyntaxException;

public class TiParser extends MySqlParserBaseVisitor {
  private final AstBuilder astBuilder;
  private TiTableInfo tableInfo;

  public TiParser() {
    astBuilder = new AstBuilder();
  }

  public TiParser(TiTableInfo tblInfo) {
    this(new AstBuilder(tblInfo));
  }

  public TiParser(AstBuilder astBuilder) {
    this.astBuilder = astBuilder;
  }

  public static TiParser createParserForPartitionWrite(TiTableInfo tblInfo) {
    return new TiParser(new PartitionWriteAstBuilder(tblInfo));
  }

  public Expression parseExpression(String command) {
    if (command.equals("")) throw new UnsupportedSyntaxException("cannot parse empty command");
    MySqlLexer lexer =
        new MySqlLexer(new CaseChangingCharStream(CharStreams.fromString(command), true));
    CommonTokenStream cmnTokStrm = new CommonTokenStream(lexer);
    MySqlParser parser = new MySqlParser(cmnTokStrm);

    try {
      // first, try parsing with potentially faster SLL mode
      parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      return astBuilder.visitExpressions(parser.expressions());
    } catch (ParseCancellationException e) {
      // if we fail, parse with LL mode
      cmnTokStrm.seek(0); // rewind input stream
      parser.reset();

      // Try Again.
      parser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return astBuilder.visitExpressions(parser.expressions());
    }
  }
}
