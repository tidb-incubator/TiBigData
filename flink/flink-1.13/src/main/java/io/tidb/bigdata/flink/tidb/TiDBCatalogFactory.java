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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * Factory for {@link TiDBCatalog}
 */
public class TiDBCatalogFactory implements CatalogFactory {

  @Override
  public String factoryIdentifier() {
    return TiDBConfigOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(
        TiDBConfigOptions.DATABASE_URL,
        TiDBConfigOptions.USERNAME
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        TiDBConfigOptions.PASSWORD,
        TiDBConfigOptions.MAX_POOL_SIZE,
        TiDBConfigOptions.MIN_IDLE_SIZE,
        TiDBConfigOptions.WRITE_MODE,
        TiDBConfigOptions.REPLICA_READ,
        TiDBConfigOptions.FILTER_PUSH_DOWN
    );
  }

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    helper.validate();
    return new TiDBCatalog(context.getName(), context.getOptions());
  }

}
