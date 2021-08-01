/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.trino.tidb;

import static com.google.common.collect.ImmutableList.toImmutableList;

import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.Wrapper;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

public final class TiDBSplitManager
    extends Wrapper<SplitManagerInternal>
    implements ConnectorSplitManager {

  @Inject
  public TiDBSplitManager(TiDBSession session) {
    super(new SplitManagerInternal(session.getInternal()));
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle table,
      SplitSchedulingStrategy splitSchedulingStrategy) {
    TiDBTableHandle tableHandle = (TiDBTableHandle) table;
    List<SplitInternal> splits = getInternal().getSplits(tableHandle.getInternal());
    return new FixedSplitSource(
        splits.stream().map(s -> new TiDBSplit(s, Optional.empty())).collect(toImmutableList()));
  }
}
