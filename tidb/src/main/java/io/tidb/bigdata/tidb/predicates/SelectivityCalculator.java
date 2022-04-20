/*
 * Copyright 2017 PingCAP, Inc.
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

package io.tidb.bigdata.tidb.predicates;

import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.expression.visitor.DefaultVisitor;
import io.tidb.bigdata.tidb.expression.visitor.PseudoCostCalculator;
import java.util.Optional;

public class SelectivityCalculator extends DefaultVisitor<Double, Void> {
  public static double calcPseudoSelectivity(ScanSpec spec) {
    Optional<Expression> rangePred = spec.getRangePredicate();
    double cost = 100.0;
    if (spec.getPointPredicates() != null) {
      for (Expression expr : spec.getPointPredicates()) {
        cost *= PseudoCostCalculator.calculateCost(expr);
      }
    }
    if (rangePred.isPresent()) {
      cost *= PseudoCostCalculator.calculateCost(rangePred.get());
    }
    return cost;
  }

  @Override
  protected Double process(Expression node, Void context) {
    return 1.0;
  }
}
