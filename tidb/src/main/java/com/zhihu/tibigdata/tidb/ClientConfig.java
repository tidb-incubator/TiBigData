/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.zhihu.tibigdata.jdbc.TiDBDriver.MYSQL_PREFIX;
import static com.zhihu.tibigdata.jdbc.TiDBDriver.TIDB_PREFIX;

import java.util.Map;
import java.util.Objects;

public final class ClientConfig {

  public static final String TIDB_DRIVER_NAME = "com.zhihu.tibigdata.jdbc.TiDBDriver";

  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  public static final String DATABASE_URL = "tidb.database.url";

  public static final String USERNAME = "tidb.username";

  public static final String PASSWORD = "tidb.password";

  public static final String MAX_POOL_SIZE = "tidb.maximum.pool.size";
  public static final int MAX_POOL_SIZE_DEFAULT = 10;

  // default value means that we connect to tidb server lazily
  public static final String MIN_IDLE_SIZE = "tidb.minimum.idle.size";
  public static final int MIN_IDLE_SIZE_DEFAULT = 10;

  private String pdAddresses;

  private String databaseUrl;

  private String username;

  private String password;

  private int maximumPoolSize;

  private int minimumIdleSize;

  public ClientConfig() {
    this(null, null, null, MAX_POOL_SIZE_DEFAULT, MIN_IDLE_SIZE_DEFAULT);
  }

  public ClientConfig(String databaseUrl, String username, String password) {
    this(databaseUrl, username, password, MAX_POOL_SIZE_DEFAULT, MIN_IDLE_SIZE_DEFAULT);
  }

  public ClientConfig(String databaseUrl, String username, String password, int maximumPoolSize,
      int minimumIdleSize) {
    this.databaseUrl = databaseUrl;
    this.username = username;
    this.password = password;
    this.maximumPoolSize = maximumPoolSize;
    this.minimumIdleSize = minimumIdleSize;
  }

  public ClientConfig(Map<String, String> properties) {
    this(properties.get(DATABASE_URL),
        properties.get(USERNAME),
        properties.get(PASSWORD),
        Integer.parseInt(
            properties.getOrDefault(MAX_POOL_SIZE, Integer.toString(MAX_POOL_SIZE_DEFAULT))),
        Integer.parseInt(
            properties.getOrDefault(MIN_IDLE_SIZE, Integer.toString(MIN_IDLE_SIZE_DEFAULT))));
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

  public String getDriverName() {
    if (databaseUrl.startsWith(MYSQL_PREFIX)) {
      return MYSQL_DRIVER_NAME;
    }
    if (databaseUrl.startsWith(TIDB_PREFIX)) {
      return TIDB_DRIVER_NAME;
    }
    throw new IllegalArgumentException("can not parse driver by " + databaseUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pdAddresses);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    ClientConfig other = (ClientConfig) obj;
    return Objects.equals(this.pdAddresses, other.pdAddresses)
        && Objects.equals(this.databaseUrl, other.databaseUrl)
        && Objects.equals(this.username, other.username)
        && Objects.equals(this.password, other.password)
        && Objects.equals(this.maximumPoolSize, other.maximumPoolSize)
        && Objects.equals(this.minimumIdleSize, other.minimumIdleSize);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("databaseUrl", databaseUrl)
        .add("username", username)
        .add("pdAddresses", pdAddresses)
        .add("maximumPoolSize", maximumPoolSize)
        .add("minimumIdleSize", minimumIdleSize)
        .toString();
  }
}