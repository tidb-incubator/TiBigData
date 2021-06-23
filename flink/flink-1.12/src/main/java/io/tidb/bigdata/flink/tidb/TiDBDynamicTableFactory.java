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
import java.util.UUID;
import java.util.function.Function;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBDynamicTableFactory extends TiDBBaseDynamicTableFactory {
  static final Logger LOG = LoggerFactory.getLogger(TiDBDynamicTableFactory.class);

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
        ExceptionUtils.suppressExceptions(clientSession::close);
      }
    }
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig options = helper.getOptions();
    final Map<String, String> properties = context.getCatalogTable().toProperties();
    final TableSchema schema = context.getCatalogTable().getSchema();
    final JdbcLookupOptions lookupOptions = getLookupOptions(context);
    return options.getOptional(STREAMING_SOURCE)
        .map(s -> createStreamingTableSource(context, s, schema, properties, lookupOptions))
        .orElseGet(() -> new TiDBBatchDynamicTableSource(schema, properties, lookupOptions));

  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return withMoreOptionalOptions(STREAMING_SOURCE);
  }

  private static String randomID() {
    return UUID.randomUUID().toString();
  }

  private static Map<String, String> kafkaDefaultParameters(Map<String, String> props) {
    props.put("properties.group.id", randomID());
    return props;
  }

  private static Map<String, String> pulsarDefaultParameters(Map<String, String> props) {
    props.put("scan.startup.mode", "external-subscription");
    props.put("scan.startup.sub-name", randomID());
    return props;
  }

  private DynamicTableSource createStreamingTableSource(Context context, String source,
      TableSchema schema, Map<String, String> properties, JdbcLookupOptions lookupOptions) {
    final Function<Map<String, String>, Map<String, String>> populateDefault;
    switch (source) {
      case STREAMING_SOURCE_KAFKA:
        populateDefault = TiDBDynamicTableFactory::kafkaDefaultParameters;
        break;
      case STREAMING_SOURCE_PULSAR:
        populateDefault = TiDBDynamicTableFactory::pulsarDefaultParameters;
        break;
      default:
        throw new IllegalStateException("Not supported TiDB streaming source: " + source);
    }
    final long version = getSnapshotTimestamp(properties);
    properties = new HashMap<>(properties);
    properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
    return new TiDBStreamingDynamicTableSource(schema, properties, lookupOptions,
        createStreamingSource(
            new StreamingSourceContext(source, context, version, populateDefault)), version);
  }

  private static Context wrapContext(final StreamingSourceContext context) {
    final CatalogTable table = context.getCatalogTable();
    final String prefix = "tidb.streaming." + context.getSourceType() + ".";

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
                formatKeyPrefix + FormatOptions.EARLIEST_VERSION.key(),
                Long.toString(context.getVersion()));
            return context.populateDefault(newProperties);
          }

          private Map<String, String> convertProperties(Map<String, String> properties) {
            Map<String, String> newProperties = newDefaultProperties(properties);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
              if (entry.getKey().startsWith(prefix)) {
                newProperties.put(entry.getKey().substring(prefix.length()), entry.getValue());
              }
            }
            LOG.info("Final properties for streaming source: {}", newProperties.toString());
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

  private static ScanTableSource createStreamingSource(final StreamingSourceContext context) {
    return (ScanTableSource) FactoryUtil.discoverFactory(
        Thread.currentThread().getContextClassLoader(),
        DynamicTableSourceFactory.class,
        context.source)
        .createDynamicTableSource(wrapContext(context));
  }

  private static class StreamingSourceContext {
    final String source;
    final Context context;
    final long version;
    final Function<Map<String, String>, Map<String, String>> populateDefault;

    private StreamingSourceContext(final String source, final Context context, final long version,
        final Function<Map<String, String>, Map<String, String>> populateDefault) {
      this.source = source;
      this.context = context;
      this.version = version;
      this.populateDefault = populateDefault;
    }

    private CatalogTable getCatalogTable() {
      return context.getCatalogTable();
    }

    private String getSourceType() {
      return source;
    }

    private long getVersion() {
      return version;
    }

    private ObjectIdentifier getObjectIdentifier() {
      return context.getObjectIdentifier();
    }

    private ReadableConfig getConfiguration() {
      return context.getConfiguration();
    }

    private ClassLoader getClassLoader() {
      return context.getClassLoader();
    }

    private boolean isTemporary() {
      return context.isTemporary();
    }

    private Map<String, String> populateDefault(Map<String, String> props) {
      return populateDefault.apply(props);
    }
  }
}
