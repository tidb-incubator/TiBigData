/*
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
 */

package io.tidb.bigdata.tidb.key;

import io.tidb.bigdata.tidb.predicates.IndexRange;
import org.tikv.common.util.Pair;

// A builder to build key range for Statistics keys
public class StatisticsKeyRangeBuilder extends KeyRangeBuilder {

  public StatisticsKeyRangeBuilder(IndexRange ir) {
    super(ir);
  }

  private Pair<Key, Key> toPairKey() {
    Key lbsKey = Key.toRawKey(lPointKey.append(lKey).getBytes());
    Key ubsKey = Key.toRawKey(uPointKey.append(uKey).getBytes());
    return new Pair<>(lbsKey, ubsKey);
  }

  public Pair<Key, Key> compute() {
    computeKeyRange();
    return toPairKey();
  }
}
