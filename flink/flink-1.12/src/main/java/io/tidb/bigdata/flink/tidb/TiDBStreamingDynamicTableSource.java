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

package io.tidb.bigdata.flink.tidb;

import io.tidb.bigdata.flink.format.cdc.CraftFormatFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.types.DataType;

public class TiDBStreamingDynamicTableSource
    extends TiDBDynamicTableSource implements SupportsReadingMetadata {

  private final ScanTableSource streamingSource;
  private StreamingReadableMetadata[] metadata;
  private final long version;

  public TiDBStreamingDynamicTableSource(
      TableSchema tableSchema,
      Map<String, String> properties,
      JdbcLookupOptions lookupOptions,
      ScanTableSource streamingSource,
      long version) {
    super(tableSchema, properties, lookupOptions);
    this.streamingSource = streamingSource;
    this.version = version;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
    return SourceFunctionProvider.of(
        new TiDBStreamingSourceFunction(getInputFormat(ctx, metadata != null ? metadata.length : 0),
            metadata, version, streamingSource.getScanRuntimeProvider(ctx)), false);
  }

  @Override
  public DynamicTableSource copy() {
    TiDBStreamingDynamicTableSource tableSource =
        new TiDBStreamingDynamicTableSource(tableSchema, properties, lookupOptions,
            (ScanTableSource) this.streamingSource.copy(), this.version);
    copyTo(tableSource);
    return tableSource;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return CraftFormatFactory.getChangelogMode();
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    final Map<String, DataType> metadataMap = new LinkedHashMap<>();
    Stream.of(StreamingReadableMetadata.values())
        .forEachOrdered(m -> metadataMap.put(m.key, m.type));
    return metadataMap;
  }

  @Override
  public void applyReadableMetadata(List<String> list, DataType dataType) {
    metadata = list.stream()
        .map(String::toUpperCase)
        .map(StreamingReadableMetadata::valueOf)
        .toArray(StreamingReadableMetadata[]::new);
    if (streamingSource instanceof SupportsReadingMetadata) {
      SupportsReadingMetadata support = (SupportsReadingMetadata) streamingSource;
      support.applyReadableMetadata(
          list.stream().map(s -> "value." + s).collect(Collectors.toList()), dataType);
    }
  }
}