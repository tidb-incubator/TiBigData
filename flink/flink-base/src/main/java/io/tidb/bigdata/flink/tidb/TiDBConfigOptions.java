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

import static io.tidb.bigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN_DEFAULT;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_DEFAULT;

import io.tidb.bigdata.tidb.ClientConfig;
import java.time.Duration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * A collection of {@link ConfigOption} which are used in tidb catalog and table
 */
public class TiDBConfigOptions {

  public static final String IDENTIFIER = "tidb";

  public static final ConfigOption<String> DATABASE_URL =
      ConfigOptions.key(ClientConfig.DATABASE_URL).stringType().noDefaultValue();

  public static final ConfigOption<String> USERNAME =
      ConfigOptions.key(ClientConfig.USERNAME).stringType().noDefaultValue();

  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key(ClientConfig.PASSWORD).stringType().noDefaultValue();

  public static final ConfigOption<String> DATABASE_NAME =
      ConfigOptions.key("tidb.database.name").stringType().noDefaultValue();

  public static final ConfigOption<String> TABLE_NAME =
      ConfigOptions.key("tidb.table.name").stringType().noDefaultValue();

  public static final ConfigOption<String> MAX_POOL_SIZE =
      ConfigOptions.key(ClientConfig.MAX_POOL_SIZE).stringType().noDefaultValue();

  public static final ConfigOption<String> MIN_IDLE_SIZE =
      ConfigOptions.key(ClientConfig.MIN_IDLE_SIZE).stringType().noDefaultValue();

  public static final ConfigOption<String> WRITE_MODE =
      ConfigOptions.key(ClientConfig.TIDB_WRITE_MODE)
          .stringType()
          .defaultValue(ClientConfig.TIDB_WRITE_MODE_DEFAULT);

  public static final ConfigOption<Boolean> REPLICA_READ =
      ConfigOptions.key(TIDB_REPLICA_READ)
          .booleanType()
          .defaultValue(TIDB_REPLICA_READ_DEFAULT);

  public static final ConfigOption<Boolean> FILTER_PUSH_DOWN =
      ConfigOptions.key(TIDB_FILTER_PUSH_DOWN)
          .booleanType()
          .defaultValue(TIDB_FILTER_PUSH_DOWN_DEFAULT);

  /**
   * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
   */
  public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
      ConfigOptions.key("sink.buffer-flush.max-rows")
          .intType()
          .defaultValue(100)
          .withDescription(
              "the flush max size (includes all append, upsert and delete records),"
                  + " over this number of records, will flush data. The default value is 100.");

  public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
      ConfigOptions.key("sink.buffer-flush.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription(
              "the flush interval mills, over this time, asynchronous threads will flush data. The "
                  + "default value is 1s.");

  public static final ConfigOption<Integer> SINK_MAX_RETRIES =
      ConfigOptions.key("sink.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("the max retry times if writing records to database failed.");

  // look up config options
  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
      ConfigOptions.key("lookup.cache.max-rows")
          .longType()
          .defaultValue(-1L)
          .withDescription("the max number of rows of lookup cache, over this value, "
              + "the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options "
              + "must all be specified if any of them is specified. "
              + "Cache is not enabled as default.");

  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
      ConfigOptions.key("lookup.cache.ttl")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription("the cache time to live.");

  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
      ConfigOptions.key("lookup.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("the max retry times if lookup database failed.");


}
