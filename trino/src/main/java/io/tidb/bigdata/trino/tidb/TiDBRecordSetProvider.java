/*
 * Copyright 2020 TiDB Project Authors.
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
import static io.tidb.bigdata.trino.tidb.TiDBConfig.SESSION_SNAPSHOT_TIMESTAMP;
import static java.util.Objects.requireNonNull;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.tikv.common.meta.TiTimestamp;

public final class TiDBRecordSetProvider implements ConnectorRecordSetProvider {

  private TiDBSession session;

  @Inject
  public TiDBRecordSetProvider(TiDBSession session) {
    this.session = session;
  }

  @Override
  public RecordSet getRecordSet(
      ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split,
      ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
    requireNonNull(split, "split is null");
    Optional<TiTimestamp> timestamp = Optional
        .ofNullable(session.getProperty(SESSION_SNAPSHOT_TIMESTAMP, String.class))
        .filter(StringUtils::isNoneEmpty)
        .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0));
    return new TiDBRecordSet(this.session, (TiDBSplit) split,
        columns.stream().map(handle -> (TiDBColumnHandle) handle).collect(toImmutableList()),
        timestamp);
  }
}
