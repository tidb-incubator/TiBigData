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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import io.tidb.bigdata.tidb.meta.TiPartitionDef;
import io.tidb.bigdata.tidb.meta.TiPartitionInfo;
import io.tidb.bigdata.tidb.meta.TiPartitionInfo.PartitionType;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import  io.tidb.bigdata.tidb.parser.TiParser;

public class PartitionPruner {
  public static List<Expression> extractLogicalOrComparisonExpr(List<Expression> filters) {
    List<Expression> filteredFilters = new ArrayList<>();
    for (Expression expr : filters) {
      if (expr instanceof LogicalBinaryExpression || expr instanceof ComparisonBinaryExpression) {
        filteredFilters.add(expr);
      }
    }
    return filteredFilters;
  }

  public static List<TiPartitionDef> prune(TiTableInfo tableInfo, List<Expression> filters) {
    PartitionType type = tableInfo.getPartitionInfo().getType();
    if (!tableInfo.isPartitionEnabled()) {
      return tableInfo.getPartitionInfo().getDefs();
    }

    boolean isRangeCol =
        Objects.requireNonNull(tableInfo.getPartitionInfo().getColumns()).size() > 0;

    switch (type) {
      case RangePartition:
        if (!isRangeCol) {
          // TiDB only supports partition pruning on range partition on single column
          // If we meet range partition on multiple columns, we simply return all parts.
          if (tableInfo.getPartitionInfo().getColumns().size() > 1) {
            return tableInfo.getPartitionInfo().getDefs();
          }
          RangePartitionPruner prunner = new RangePartitionPruner(tableInfo);
          return prunner.prune(filters);
        } else {
          RangeColumnPartitionPruner pruner = new RangeColumnPartitionPruner(tableInfo);
          return pruner.prune(filters);
        }
      case ListPartition:
      case HashPartition:
        return tableInfo.getPartitionInfo().getDefs();
    }

    throw new UnsupportedOperationException("cannot prune under invalid partition table");
  }

  static void generateRangeExprs(
      TiPartitionInfo partInfo,
      List<Expression> partExprs,
      TiParser parser,
      String partExprStr,
      int lessThanIdx) {
    // partExprColRefs.addAll(PredicateUtils.extractColumnRefFromExpression(partExpr));
    for (int i = 0; i < partInfo.getDefs().size(); i++) {
      TiPartitionDef pDef = partInfo.getDefs().get(i);
      String current = pDef.getLessThan().get(lessThanIdx);
      String leftHand;
      if (current.equals("MAXVALUE")) {
        leftHand = "true";
      } else {
        leftHand = String.format("%s < %s", partExprStr, current);
      }
      if (i == 0) {
        partExprs.add(parser.parseExpression(leftHand));
      } else {
        String previous = partInfo.getDefs().get(i - 1).getLessThan().get(lessThanIdx);
        String and = String.format("%s and %s", partExprStr + ">=" + previous, leftHand);
        partExprs.add(parser.parseExpression(and));
      }
    }
  }
}
