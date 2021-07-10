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

package io.tidb.bigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.tidb.bigdata.jdbc.TiDBDriver.MYSQL_PREFIX;
import static io.tidb.bigdata.jdbc.TiDBDriver.TIDB_PREFIX;

import com.google.common.base.Objects;
import java.util.Map;

public final class ClientConfig {

  public static final String TIDB_DRIVER_NAME = "io.tidb.bigdata.jdbc.TiDBDriver";

  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  public static final String DATABASE_URL = "tidb.database.url";

  public static final String USERNAME = "tidb.username";

  public static final String PASSWORD = "tidb.password";

  public static final String MAX_POOL_SIZE = "tidb.maximum.pool.size";
  public static final int MAX_POOL_SIZE_DEFAULT = 10;

  // default value means that we connect to tidb server lazily
  public static final String MIN_IDLE_SIZE = "tidb.minimum.idle.size";
  public static final int MIN_IDLE_SIZE_DEFAULT = 10;

  public static final String TIDB_WRITE_MODE = "tidb.write_mode";
  public static final String TIDB_WRITE_MODE_DEFAULT = "append";

  public static final String TIDB_REPLICA_READ = "tidb.replica-read";
  public static final String TIDB_REPLICA_READ_LEADER = "leader";
  public static final String TIDB_REPLICA_READ_FOLLOWER = "follower";
  public static final String TIDB_REPLICA_READ_LEADER_AND_FOLLOWER = "leader_and_follower";
  public static final String TIDB_REPLICA_READ_DEFAULT = TIDB_REPLICA_READ_LEADER;

  public static final String TIDB_REPLICA_READ_LABEL = "tidb.replica-read.label";
  public static final String TIDB_REPLICA_READ_LABEL_DEFAULT = "";

  public static final String TIDB_REPLICA_READ_ADDRESS_WHITELIST =
      "tidb.replica-read.address.whitelist";
  public static final String TIDB_REPLICA_READ_ADDRESS_BLACKLIST =
      "tidb.replica-read.address.blacklist";
  public static final String TIDB_REPLICA_READ_ADDRESS_DEFAULT = "";

  public static final String TIDB_FILTER_PUSH_DOWN = "tidb.filter-push-down";
  public static final boolean TIDB_FILTER_PUSH_DOWN_DEFAULT = false;

  public static final String SNAPSHOT_TIMESTAMP = "tidb.snapshot_timestamp";
  public static final String SNAPSHOT_VERSION = "tidb.snapshot_version";

  public static final String TIDB_DNS_SEARCH = "tidb.dns.search";
  public static final String TIDB_DNS_SEARCH_DEFAULT = null;

  private String pdAddresses;

  private String databaseUrl;

  private String username;

  private String password;

  private int maximumPoolSize;

  private int minimumIdleSize;

  private String writeMode;

  private final ReplicaReadPolicy replicaReadPolicy;

  private boolean isFilterPushDown;

  private String dnsSearch;

  public boolean isFilterPushDown() {
    return isFilterPushDown;
  }

  public void setFilterPushDown(boolean filterPushDown) {
    isFilterPushDown = filterPushDown;
  }

  public final ReplicaReadPolicy getReplicaReadPolicy() {
    return replicaReadPolicy;
  }

  public ClientConfig() {
    this(null, null, null, MAX_POOL_SIZE_DEFAULT,
        MIN_IDLE_SIZE_DEFAULT, TIDB_WRITE_MODE_DEFAULT,
        ReplicaReadPolicy.DEFAULT, TIDB_FILTER_PUSH_DOWN_DEFAULT,
        TIDB_DNS_SEARCH_DEFAULT);
  }

  public ClientConfig(String databaseUrl, String username, String password) {
    this(databaseUrl, username, password, MAX_POOL_SIZE_DEFAULT, MIN_IDLE_SIZE_DEFAULT,
        TIDB_WRITE_MODE_DEFAULT, ReplicaReadPolicy.DEFAULT, TIDB_FILTER_PUSH_DOWN_DEFAULT,
        TIDB_DNS_SEARCH_DEFAULT);
  }

  public ClientConfig(String databaseUrl, String username, String password, int maximumPoolSize,
      int minimumIdleSize, String writeMode, ReplicaReadPolicy replicaRead,
      boolean isFilterPushDown, String dnsSearch) {
    this.databaseUrl = databaseUrl;
    this.username = username;
    this.password = password;
    this.maximumPoolSize = maximumPoolSize;
    this.minimumIdleSize = minimumIdleSize;
    this.writeMode = writeMode;
    this.replicaReadPolicy = replicaRead;
    this.isFilterPushDown = isFilterPushDown;
    this.dnsSearch = dnsSearch;
  }

  public ClientConfig(Map<String, String> properties) {
    this(properties.get(DATABASE_URL),
        properties.get(USERNAME),
        properties.get(PASSWORD),
        Integer.parseInt(
            properties.getOrDefault(MAX_POOL_SIZE, Integer.toString(MAX_POOL_SIZE_DEFAULT))),
        Integer.parseInt(
            properties.getOrDefault(MIN_IDLE_SIZE, Integer.toString(MIN_IDLE_SIZE_DEFAULT))),
        properties.getOrDefault(TIDB_WRITE_MODE, TIDB_WRITE_MODE_DEFAULT),
        ReplicaReadPolicy.create(properties),
        Boolean.parseBoolean(properties
            .getOrDefault(TIDB_FILTER_PUSH_DOWN, Boolean.toString(TIDB_FILTER_PUSH_DOWN_DEFAULT))),
        properties.getOrDefault(TIDB_DNS_SEARCH, TIDB_DNS_SEARCH_DEFAULT)
    );
  }

  public ClientConfig(ClientConfig config) {
    this(config.getDatabaseUrl(),
        config.getUsername(),
        config.getPassword(),
        config.getMaximumPoolSize(),
        config.getMinimumIdleSize(),
        config.getWriteMode(),
        config.getReplicaReadPolicy(),
        config.isFilterPushDown(),
        config.getDnsSearch());
  }

  public String getPdAddresses() {
    return pdAddresses;
  }

  public void setPdAddresses(String addresses) {
    this.pdAddresses = addresses;
  }

  public String getDatabaseUrl() {
    return databaseUrl;
  }

  public void setDatabaseUrl(String databaseUrl) {
    this.databaseUrl = databaseUrl;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public int getMaximumPoolSize() {
    return maximumPoolSize;
  }

  public void setMaximumPoolSize(int maximumPoolSize) {
    this.maximumPoolSize = maximumPoolSize;
  }

  public int getMinimumIdleSize() {
    return minimumIdleSize;
  }

  public void setMinimumIdleSize(int minimumIdleSize) {
    this.minimumIdleSize = minimumIdleSize;
  }

  public String getWriteMode() {
    return writeMode;
  }

  public void setWriteMode(String writeMode) {
    this.writeMode = writeMode;
  }

  public String getDriverName() {
    if (databaseUrl.startsWith(MYSQL_PREFIX)) {
      return MYSQL_DRIVER_NAME;
    }
    if (databaseUrl.startsWith(TIDB_PREFIX)) {
      return TIDB_DRIVER_NAME;
    }
    throw new IllegalArgumentException("can not parse driver by " + databaseUrl);
  }

  public String getDnsSearch() {
    return dnsSearch;
  }

  public void setDnsSearch(String dnsSearch) {
    this.dnsSearch = dnsSearch;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pdAddresses, databaseUrl, username, password, maximumPoolSize,
        minimumIdleSize, writeMode, replicaReadPolicy);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    ClientConfig that = (ClientConfig) object;
    return maximumPoolSize == that.maximumPoolSize
        && minimumIdleSize == that.minimumIdleSize
        && Objects.equal(replicaReadPolicy, that.replicaReadPolicy)
        && Objects.equal(pdAddresses, that.pdAddresses)
        && Objects.equal(databaseUrl, that.databaseUrl)
        && Objects.equal(username, that.username)
        && Objects.equal(password, that.password)
        && Objects.equal(writeMode, that.writeMode);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("databaseUrl", databaseUrl)
        .add("username", username)
        .add("pdAddresses", pdAddresses)
        .add("maximumPoolSize", maximumPoolSize)
        .add("minimumIdleSize", minimumIdleSize)
        .add("writeMode", writeMode)
        .add("replicaReadPolicy", replicaReadPolicy)
        .toString();
  }
}
