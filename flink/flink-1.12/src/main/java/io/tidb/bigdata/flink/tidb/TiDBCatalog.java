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

import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.factories.Factory;

public class TiDBCatalog extends TiDBBaseCatalog {

  public TiDBCatalog(String name, String defaultDatabase, Map<String, String> properties) {
    super(name, defaultDatabase, properties);
  }

  public TiDBCatalog(String name, Map<String, String> properties) {
    super(name, properties);
  }

  public TiDBCatalog(Map<String, String> properties) {
    super(properties);
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new TiDBDynamicTableFactory());
  }
}
