/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector;

import io.tidb.bigdata.flink.connector.sink.TiDBDataStreamSinkProvider;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

public class TiDBDynamicTableSink implements DynamicTableSink {

  private final ResolvedCatalogTable table;
  private final String databaseName;
  private final String tableName;
  private final TiDBSinkOptions sinkOptions;

  public TiDBDynamicTableSink(String databaseName, String tableName, ResolvedCatalogTable table,
      TiDBSinkOptions tiDBSinkOptions) {
    this.table = table;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.sinkOptions = tiDBSinkOptions;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return new TiDBDataStreamSinkProvider(databaseName, tableName, table, context, sinkOptions);
  }

  @Override
  public DynamicTableSink copy() {
    return new TiDBDynamicTableSink(databaseName, tableName, table, sinkOptions);
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }
}