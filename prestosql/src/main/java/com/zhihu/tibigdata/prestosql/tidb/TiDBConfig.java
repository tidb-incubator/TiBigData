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

package com.zhihu.tibigdata.prestosql.tidb;

import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.Wrapper;
import io.airlift.configuration.Config;

public final class TiDBConfig extends Wrapper<ClientConfig> {

  // for properties
  public static final String WRITE_MODE_GLOBAL = "tidb.write_mode";

  // for session
  public static final String WRITE_MODE = "write_mode";

  // for table properties
  public static final String PRIMARY_KEY = "primary_key";

  private String writeMode = "append";

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
    return writeMode;
  }

  @Config(WRITE_MODE_GLOBAL)
  public TiDBConfig setWriteMode(String writeMode) {
    this.writeMode = writeMode;
    return this;
  }
}
