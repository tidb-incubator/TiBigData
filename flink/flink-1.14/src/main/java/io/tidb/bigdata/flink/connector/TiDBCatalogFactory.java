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

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

/** Factory for {@link TiDBCatalog} */
public class TiDBCatalogFactory implements CatalogFactory {

  public static final String IDENTIFIER = "tidb";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return TiDBOptions.requiredOptions();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    // The options may less than real properties which tidb supported,
    // just use it by create catalog sql, we will not verify properties by flink api.
    return TiDBOptions.optionalOptions();
  }

  @Override
  public Catalog createCatalog(Context context) {
    return new TiDBCatalog(context.getName(), context.getOptions());
  }
}
