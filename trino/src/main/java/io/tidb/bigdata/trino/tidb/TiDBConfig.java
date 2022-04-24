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

import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_ADDRESS_BLACKLIST;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_ADDRESS_DEFAULT;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_ADDRESS_WHITELIST;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_DEFAULT;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_LABEL;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ_LABEL_DEFAULT;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;

import io.airlift.configuration.Config;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ReplicaReadPolicy;
import io.tidb.bigdata.tidb.Wrapper;
import java.util.HashMap;
import java.util.Map;

public final class TiDBConfig extends Wrapper<ClientConfig> {

  // for session
  public static final String SESSION_WRITE_MODE = "write_mode";
  public static final String SESSION_SNAPSHOT_TIMESTAMP = "snapshot_timestamp";

  // for table properties
  public static final String PRIMARY_KEY = "primary_key";

  // for table properties
  public static final String UNIQUE_KEY = "unique_key";

  // for replica read
  private final Map<String, String> replicaReadProperties = new HashMap<>();

  public TiDBConfig() {
    super(new ClientConfig());
  }

  public String getDatabaseUrl() {
    return getInternal().getDatabaseUrl();
  }

  @Config(ClientConfig.DATABASE_URL)
  public TiDBConfig setDatabaseUrl(String databasesUrl) {
    getInternal().setDatabaseUrl(databasesUrl);
    return this;
  }

  public String getUsername() {
    return getInternal().getUsername();
  }

  @Config(ClientConfig.USERNAME)
  public TiDBConfig setUsername(String username) {
    getInternal().setUsername(username);
    return this;
  }

  public String getPassword() {
    return getInternal().getPassword();
  }

  @Config(ClientConfig.PASSWORD)
  public TiDBConfig setPassword(String password) {
    getInternal().setPassword(password);
    return this;
  }

  public int getMaximumPoolSize() {
    return getInternal().getMaximumPoolSize();
  }

  @Config(ClientConfig.MAX_POOL_SIZE)
  public TiDBConfig setMaximumPoolSize(int maximumPoolSize) {
    getInternal().setMaximumPoolSize(maximumPoolSize);
    return this;
  }

  public int getMinimumIdle() {
    return getInternal().getMinimumIdleSize();
  }

  @Config(ClientConfig.MIN_IDLE_SIZE)
  public TiDBConfig setMinimumIdle(int minimumIdle) {
    getInternal().setMinimumIdleSize(minimumIdle);
    return this;
  }

  public String getWriteMode() {
    return getInternal().getWriteMode();
  }

  @Config(TIDB_WRITE_MODE)
  public TiDBConfig setWriteMode(String writeMode) {
    getInternal().setWriteMode(writeMode);
    return this;
  }

  @Config(TIDB_REPLICA_READ)
  public TiDBConfig setReplicaRead(String replicaRead) {
    this.replicaReadProperties.put(TIDB_REPLICA_READ, replicaRead);
    getInternal().setReplicaReadPolicy(ReplicaReadPolicy.create(replicaReadProperties));
    return this;
  }

  public String getReplicaRead() {
    return this.replicaReadProperties.getOrDefault(TIDB_REPLICA_READ, TIDB_REPLICA_READ_DEFAULT);
  }

  @Config(TIDB_REPLICA_READ_LABEL)
  public TiDBConfig setReplicaReadLabel(String replicaReadLabel) {
    this.replicaReadProperties.put(TIDB_REPLICA_READ_LABEL, replicaReadLabel);
    getInternal().setReplicaReadPolicy(ReplicaReadPolicy.create(replicaReadProperties));
    return this;
  }

  public String getReplicaReadLabel() {
    return this.replicaReadProperties.getOrDefault(
        TIDB_REPLICA_READ_LABEL, TIDB_REPLICA_READ_LABEL_DEFAULT);
  }

  @Config(TIDB_REPLICA_READ_ADDRESS_WHITELIST)
  public TiDBConfig setReplicaReadAddressWhitelist(String whitelist) {
    this.replicaReadProperties.put(TIDB_REPLICA_READ_ADDRESS_WHITELIST, whitelist);
    getInternal().setReplicaReadPolicy(ReplicaReadPolicy.create(replicaReadProperties));
    return this;
  }

  public String getReplicaReadAddressWhitelist() {
    return this.replicaReadProperties.getOrDefault(
        TIDB_REPLICA_READ_ADDRESS_WHITELIST, TIDB_REPLICA_READ_ADDRESS_DEFAULT);
  }

  @Config(TIDB_REPLICA_READ_ADDRESS_BLACKLIST)
  public TiDBConfig setReplicaReadAddressBlacklist(String blacklist) {
    this.replicaReadProperties.put(TIDB_REPLICA_READ_ADDRESS_BLACKLIST, blacklist);
    getInternal().setReplicaReadPolicy(ReplicaReadPolicy.create(replicaReadProperties));
    return this;
  }

  public String getReplicaReadAddressBlacklist() {
    return this.replicaReadProperties.getOrDefault(
        TIDB_REPLICA_READ_ADDRESS_BLACKLIST, TIDB_REPLICA_READ_ADDRESS_DEFAULT);
  }
}
