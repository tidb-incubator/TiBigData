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

package io.tidb.bigdata.flink.connector.source;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.tidb.ClientConfig;
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

  // Options for TLS
  public static final ConfigOption<String> CLUSTER_TLS_ENABLE =
      optional(ClientConfig.CLUSTER_TLS_ENABLE);
  public static final ConfigOption<String> CLUSTER_TLS_CA = optional(ClientConfig.CLUSTER_TLS_CA);
  public static final ConfigOption<String> CLUSTER_TLS_KEY = optional(ClientConfig.CLUSTER_TLS_KEY);
  public static final ConfigOption<String> CLUSTER_TLS_CERT =
      optional(ClientConfig.CLUSTER_TLS_CERT);
  public static final ConfigOption<String> CLUSTER_JKS_ENABLE =
      optional(ClientConfig.CLUSTER_JKS_ENABLE);
  public static final ConfigOption<String> CLUSTER_JKS_KEY_PATH =
      optional(ClientConfig.CLUSTER_JKS_KEY_PATH);
  public static final ConfigOption<String> CLUSTER_JKS_KEY_PASSWORD =
      optional(ClientConfig.CLUSTER_JKS_KEY_PASSWORD);
  public static final ConfigOption<String> CLUSTER_JKS_TRUST_PATH =
      optional(ClientConfig.CLUSTER_JKS_TRUST_PATH);
  public static final ConfigOption<String> CLUSTER_JKS_TRUST_PASSWORD =
      optional(ClientConfig.CLUSTER_JKS_TRUST_PASSWORD);

  // For example:
  // 'tidb.metadata.included' = 'commit_timestamp=_commit_timestamp,commit_version=_commit_version'
  public static final String METADATA_INCLUDED = "tidb.metadata.included";
  public static final String METADATA_INCLUDED_ALL = "*";

  public static Set<ConfigOption<?>> requiredOptions() {
    return withMoreRequiredOptions();
  }

  public static Set<ConfigOption<?>> withMoreRequiredOptions(ConfigOption<?>... options) {
    return ImmutableSet.<ConfigOption<?>>builder()
        .add(DATABASE_URL, DATABASE_NAME, TABLE_NAME, USERNAME)
        .add(options)
        .build();
  }

  public static Set<ConfigOption<?>> optionalOptions() {
    return withMoreOptionalOptions();
  }

  public static Set<ConfigOption<?>> withMoreOptionalOptions(ConfigOption<?>... options) {
    return ImmutableSet.<ConfigOption<?>>builder()
        .add(
            PASSWORD,
            MAX_POOL_SIZE,
            MIN_IDLE_SIZE,
            STREAMING_SOURCE,
            WRITE_MODE,
            SOURCE_FAILOVER,
            CLUSTER_TLS_ENABLE,
            CLUSTER_TLS_CA,
            CLUSTER_TLS_KEY,
            CLUSTER_TLS_CERT,
            CLUSTER_JKS_ENABLE,
            CLUSTER_JKS_KEY_PATH,
            CLUSTER_JKS_KEY_PASSWORD,
            CLUSTER_JKS_TRUST_PATH,
            CLUSTER_JKS_TRUST_PASSWORD)
        .add(options)
        .build();
  }
}
