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

import static io.tidb.bigdata.trino.tidb.TiDBConfig.PRIMARY_KEY;
import static io.tidb.bigdata.trino.tidb.TiDBConfig.SESSION_SNAPSHOT_TIMESTAMP;
import static io.tidb.bigdata.trino.tidb.TiDBConfig.SESSION_WRITE_MODE;
import static io.tidb.bigdata.trino.tidb.TiDBConfig.UNIQUE_KEY;
import static io.trino.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import javax.inject.Inject;

public final class TiDBConnector implements Connector {

  private static final Logger log = Logger.get(TiDBConnector.class);

  private final TiDBConfig config;
  private final LifeCycleManager lifeCycleManager;
  private final TiDBMetadata metadata;
  private final TiDBSplitManager splitManager;
  private final TiDBRecordSetProvider recordSetProvider;
  private final TiDBPageSinkProvider pageSinkProvider;

  @Inject
  public TiDBConnector(
      TiDBConfig config,
      LifeCycleManager lifeCycleManager,
      TiDBMetadata metadata,
      TiDBSplitManager splitManager,
      TiDBRecordSetProvider recordSetProvider,
      TiDBPageSinkProvider pageSinkProvider) {
    this.config = requireNonNull(config, "config is null");
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly) {
    checkConnectorSupports(REPEATABLE_READ, isolationLevel);
    return new TiDBTransactionHandle();
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    return recordSetProvider;
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    return pageSinkProvider;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return ImmutableList.of(
        PropertyMetadata.stringProperty(PRIMARY_KEY, "tidb table primary key", "", false),
        PropertyMetadata.stringProperty(UNIQUE_KEY, "tidb table unique key", "", false));
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return ImmutableList.of(
        PropertyMetadata.stringProperty(
            SESSION_WRITE_MODE,
            "tidb sink write mode: append or upsert",
            config.getWriteMode(),
            false),
        PropertyMetadata.stringProperty(
            SESSION_SNAPSHOT_TIMESTAMP, "timestamp for snapshot read", null, false));
  }

  @Override
  public final void shutdown() {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      log.error(e, "Error shutting down connector");
    }
  }
}
