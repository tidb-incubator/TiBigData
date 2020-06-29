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

package com.zhihu.prestosql.tidb;

import com.zhihu.presto.tidb.ClientConfig;
import com.zhihu.presto.tidb.Wrapper;
import io.airlift.configuration.Config;

public final class TiDBConfig extends Wrapper<ClientConfig> {

  public TiDBConfig() {
    super(new ClientConfig());
  }

  public String getPdAddresses() {
    return getInternal().getPdAddresses();
  }

  @Config("presto.tidb.pd.addresses")
  public TiDBConfig setPdAddresses(String addresses) {
    getInternal().setPdAddresses(addresses);
    return this;
  }
}
