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

import io.tidb.bigdata.cdc.Key.Type;
import io.tidb.bigdata.flink.format.cdc.CraftFormatFactory;
import io.tidb.bigdata.flink.format.cdc.FormatOptions;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.util.ExceptionUtils;

public class TiDBDynamicTableFactory extends TiDBBaseDynamicTableFactory {
  public static final ConfigOption<Boolean> STREAMING_MODE = ConfigOptions
      .key("tidb.streaming").booleanType().defaultValue(false);

  public static final ConfigOption<String> STREAMING_SOURCE = ConfigOptions
      .key("tidb.streaming.source").stringType().noDefaultValue();

  private static final String STREAMING_SOURCE_KAFKA = "kafka";
  private static final String STREAMING_SOURCE_PULSAR = "pulsar";

  private static long getSnapshotTimestamp(Map<String, String> properties) {
    ClientSession clientSession = null;
    try {
      clientSession = ClientSession.createWithSingleConnection(new ClientConfig(properties));
      return clientSession.getSnapshotVersion().getVersion();
    } finally {
      if (clientSession != null) {
        final ClientSession session = clientSession;
        ExceptionUtils.suppressExceptions(() -> session.close());
      }
    }
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig options = helper.getOptions();
    Optional<ScanTableSource> streamingSource = Optional.empty();
    Map<String, String> properties = context.getCatalogTable().toProperties();
    if (options.get(STREAMING_MODE).booleanValue()) {
      final String source = options.getOptional(STREAMING_SOURCE)
          .map(String::toLowerCase)
          .orElseThrow(IllegalArgumentException::new);
      final long version;
      switch (source) {
        case STREAMING_SOURCE_KAFKA:
          // FALLTHROUGH
        case STREAMING_SOURCE_PULSAR:
          version = getSnapshotTimestamp(properties);
          streamingSource = Optional.of(createStreamingSource(source, context, version));
          break;
        default:
          throw new IllegalStateException("Not supported TiDB streaming source: " + source);
      }
      properties = new HashMap<>(properties);
      properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
    }

    TableSchema schema = context.getCatalogTable().getSchema();
    return new TiDBDynamicTableSource(
        schema, properties, getLookupOptions(context), streamingSource);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return optionalOptions(STREAMING_MODE, STREAMING_SOURCE);
  }

  private static Context wrapContext(final Context context, final String type, final long version) {
    final CatalogTable table = context.getCatalogTable();
    final String prefix = "tidb.streaming." + type + ".";

    return new Context() {
      @Override
      public ObjectIdentifier getObjectIdentifier() {
        return context.getObjectIdentifier();
      }

      @Override
      public CatalogTable getCatalogTable() {
        return new CatalogTable() {
          @Override
          public boolean isPartitioned() {
            return table.isPartitioned();
          }

          @Override
          public List<String> getPartitionKeys() {
            return table.getPartitionKeys();
          }

          @Override
          public CatalogTable copy(Map<String, String> map) {
            return table.copy(map);
          }

          @Override
          public CatalogBaseTable copy() {
            return table.copy();
          }

          @Override
          public Map<String, String> toProperties() {
            return convertProperties(table.toProperties());
          }

          @Override
          public Map<String, String> getProperties() {
            return convertProperties(table.getProperties());
          }

          private void populateDefault(Map<String, String> src, String srcKey,
              Map<String, String> dest, String destKey) {
            if (src.containsKey(srcKey)) {
              dest.put(destKey, src.get(srcKey));
            }
          }

          private Map<String, String> newDefaultProperties(Map<String, String> src) {
            HashMap<String, String> newProperties = new HashMap<>();
            String formatKeyPrefix = "value." + CraftFormatFactory.IDENTIFIER + ".";
            populateDefault(src, DATABASE_NAME.key(),
                newProperties, formatKeyPrefix + FormatOptions.SCHEMA_INCLUDE.key());
            populateDefault(src, TABLE_NAME.key(),
                newProperties, formatKeyPrefix + FormatOptions.TABLE_INCLUDE.key());
            newProperties.put("value.format", CraftFormatFactory.IDENTIFIER);
            newProperties.put(
                formatKeyPrefix + FormatOptions.TYPE_INCLUDE.key(), Type.ROW_CHANGED.name());
            newProperties.put(
                formatKeyPrefix + FormatOptions.EARLIEST_VERSION.key(), Long.toString(version));
            return newProperties;
          }

          private Map<String, String> convertProperties(Map<String, String> properties) {
            Map<String, String> newProperties = newDefaultProperties(properties);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
              if (entry.getKey().startsWith(prefix)) {
                newProperties.put(entry.getKey().substring(prefix.length()), entry.getValue());
              }
            }
            return newProperties;
          }

          @Override
          public TableSchema getSchema() {
            return table.getSchema();
          }

          @Override
          public String getComment() {
            return table.getComment();
          }

          @Override
          public Optional<String> getDescription() {
            return table.getDescription();
          }

          @Override
          public Optional<String> getDetailedDescription() {
            return table.getDetailedDescription();
          }
        };
      }

      @Override
      public ReadableConfig getConfiguration() {
        return context.getConfiguration();
      }

      @Override
      public ClassLoader getClassLoader() {
        return context.getClassLoader();
      }

      @Override
      public boolean isTemporary() {
        return context.isTemporary();
      }
    };
  }

  private static ScanTableSource createStreamingSource(final String source,
      final Context context, final long version) {
    return (ScanTableSource) FactoryUtil.discoverFactory(
        Thread.currentThread().getContextClassLoader(),
        DynamicTableSourceFactory.class,
        source)
        .createDynamicTableSource(wrapContext(context, source, version));
  }
}
