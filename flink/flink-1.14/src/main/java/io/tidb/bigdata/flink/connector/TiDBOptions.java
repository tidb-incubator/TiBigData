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

package io.tidb.bigdata.flink.connector;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl.JDBC;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.tidb.ClientConfig;
import java.util.Arrays;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TiDBOptions {

  private static ConfigOption<String> required(String key) {
    return ConfigOptions.key(key).stringType().noDefaultValue();
  }

  private static ConfigOption<String> optional(String key, String value) {
    return ConfigOptions.key(key).stringType().defaultValue(value);
  }

  private static ConfigOption<String> optional(String key) {
    return optional(key, null);
  }

  public static final ConfigOption<String> DATABASE_URL = required(ClientConfig.DATABASE_URL);

  public static final ConfigOption<String> USERNAME = required(ClientConfig.USERNAME);

  public static final ConfigOption<String> PASSWORD = required(ClientConfig.PASSWORD);

  public static final ConfigOption<String> DATABASE_NAME = required("tidb.database.name");

  public static final ConfigOption<String> TABLE_NAME = required("tidb.table.name");

  public static final ConfigOption<String> MAX_POOL_SIZE = required(ClientConfig.MAX_POOL_SIZE);

  public static final ConfigOption<String> MIN_IDLE_SIZE = required(ClientConfig.MIN_IDLE_SIZE);

  public static final ConfigOption<String> WRITE_MODE =
      optional(ClientConfig.TIDB_WRITE_MODE, ClientConfig.TIDB_WRITE_MODE_DEFAULT);

  public static final ConfigOption<String> REPLICA_READ =
      optional(ClientConfig.TIDB_REPLICA_READ, ClientConfig.TIDB_REPLICA_READ_DEFAULT);

  public static final ConfigOption<Boolean> FILTER_PUSH_DOWN =
      ConfigOptions.key(ClientConfig.TIDB_FILTER_PUSH_DOWN)
          .booleanType()
          .defaultValue(Boolean.parseBoolean(ClientConfig.TIDB_FILTER_PUSH_DOWN_DEFAULT));

  public static final ConfigOption<String> DNS_SEARCH = optional(ClientConfig.TIDB_DNS_SEARCH);

  public static final ConfigOption<String> SNAPSHOT_TIMESTAMP =
      optional(ClientConfig.SNAPSHOT_TIMESTAMP);

  public static final ConfigOption<String> SNAPSHOT_VERSION =
      optional(ClientConfig.SNAPSHOT_VERSION);

  public static final ConfigOption<SinkImpl> SINK_IMPL =
      ConfigOptions.key("tidb.sink.impl").enumType(SinkImpl.class).defaultValue(JDBC);
  public static final ConfigOption<SinkTransaction> SINK_TRANSACTION =
      ConfigOptions.key("tikv.sink.transaction")
          .enumType(SinkTransaction.class)
          .defaultValue(MINIBATCH);

  public static final ConfigOption<Integer> SINK_BUFFER_SIZE =
      ConfigOptions.key("tikv.sink.buffer-size").intType().defaultValue(1000);
  public static final ConfigOption<Integer> ROW_ID_ALLOCATOR_STEP =
      ConfigOptions.key("tikv.sink.row-id-allocator.step").intType().defaultValue(30000);
  public static final ConfigOption<Boolean> IGNORE_AUTOINCREMENT_COLUMN_VALUE =
      ConfigOptions.key("tikv.sink.ignore-autoincrement-column-value")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "If true, "
                  + "for autoincrement column, we will generate value instead of the the actual value. "
                  + "And if false, the value of autoincrement column can not be null");
  public static final ConfigOption<Boolean> DEDUPLICATE =
      ConfigOptions.key("tikv.sink.deduplicate")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether deduplicate row by unique key");
  public static final ConfigOption<Long> TASK_START_INTERVAL =
      ConfigOptions.key("tikv.sink.task-start-interval")
          .longType()
          .defaultValue(1000L)
          .withDescription(
              "The interval between two task start, in milliseconds, in oder to avoid allocate rowId conflict");
  public static final ConfigOption<String> UPDATE_COLUMNS =
      ConfigOptions.key("tidb.sink.update-columns")
          .stringType()
          .noDefaultValue()
          .withDescription("The columns to be updated");

  // split or offset
  public static final ConfigOption<String> SOURCE_FAILOVER =
      optional("tidb.source.failover", "split");

  public static final ConfigOption<String> STREAMING_SOURCE = optional("tidb.streaming.source");

  public static final String STREAMING_SOURCE_KAFKA = "kafka";

  public static final Set<String> VALID_STREAMING_SOURCES = ImmutableSet.of(STREAMING_SOURCE_KAFKA);

  public static final ConfigOption<String> STREAMING_CODEC = optional("tidb.streaming.codec");

  public static final String STREAMING_CODEC_JSON = "json";
  public static final String STREAMING_CODEC_CRAFT = "craft";
  public static final String STREAMING_CODEC_CANAL_JSON = "canal-json";
  public static final Set<String> VALID_STREAMING_CODECS =
      ImmutableSet.of(STREAMING_CODEC_CRAFT, STREAMING_CODEC_JSON, STREAMING_CODEC_CANAL_JSON);

  // Options for catalog
  public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
      ConfigOptions.key("tidb.streaming.ignore-parse-errors")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Optional flag to skip change events with parse errors instead of failing;\n"
                  + "fields are set to null in case of errors, false by default.");

  // For example:
  // 'tidb.metadata.included' = 'commit_timestamp=_commit_timestamp,commit_version=_commit_version'
  public static final String METADATA_INCLUDED = "tidb.metadata.included";
  public static final String METADATA_INCLUDED_ALL = "*";

  /**
   * see {@link org.apache.flink.connector.jdbc.table.JdbcConnectorOptions}
   *
   * @return
   */
  private static Set<ConfigOption<?>> jdbcOptionalOptions() {
    return ImmutableSet.of(
        SINK_PARALLELISM,
        MAX_RETRY_TIMEOUT,
        LOOKUP_CACHE_MAX_ROWS,
        LOOKUP_CACHE_TTL,
        LOOKUP_MAX_RETRIES,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_MAX_RETRIES);
  }

  public static Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(DATABASE_URL, USERNAME);
  }

  public static Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.<ConfigOption<?>>builder()
        .addAll(jdbcOptionalOptions())
        .add(
            PASSWORD,
            REPLICA_READ,
            FILTER_PUSH_DOWN,
            MAX_POOL_SIZE,
            MIN_IDLE_SIZE,
            STREAMING_SOURCE,
            WRITE_MODE,
            SINK_IMPL,
            SINK_TRANSACTION,
            SINK_BUFFER_SIZE,
            ROW_ID_ALLOCATOR_STEP,
            IGNORE_AUTOINCREMENT_COLUMN_VALUE,
            DEDUPLICATE,
            SOURCE_FAILOVER)
        .build();
  }

  public enum SinkImpl {
    JDBC,
    TIKV;

    public static SinkImpl fromString(String s) {
      for (SinkImpl value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.impl must be one of: " + Arrays.toString(values()));
    }
  }

  public enum SinkTransaction {
    GLOBAL,
    MINIBATCH,
    CHECKPOINT;

    public static SinkTransaction fromString(String s) {
      for (SinkTransaction value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.transaction must be one of: " + Arrays.toString(values()));
    }
  }
}
