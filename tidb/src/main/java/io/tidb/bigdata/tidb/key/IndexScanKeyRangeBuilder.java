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

import static org.tikv.common.util.KeyRangeUtils.makeCoprocRange;

import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.predicates.IndexRange;
import org.tikv.kvproto.Coprocessor.KeyRange;

// IndexScanKeyRangeBuilder accepts a table id, an index info, and an index range.
// With these info, it can build a key range which can be used for index scan.
// TODO: more refactoring on the way
public class IndexScanKeyRangeBuilder extends KeyRangeBuilder {
  private final long id;
  private final TiIndexInfo index;

  public IndexScanKeyRangeBuilder(long id, TiIndexInfo index, IndexRange ir) {
    super(ir);
    this.id = id;
    this.index = index;
  }

  private KeyRange toPairKey() {
    IndexKey lbsKey = IndexKey.toIndexKey(id, index.getId(), lPointKey, lKey);
    IndexKey ubsKey = IndexKey.toIndexKey(id, index.getId(), uPointKey, uKey);
    return makeCoprocRange(lbsKey.toByteString(), ubsKey.toByteString());
  }

  public KeyRange compute() {
    computeKeyRange();
    return toPairKey();
  }
}
