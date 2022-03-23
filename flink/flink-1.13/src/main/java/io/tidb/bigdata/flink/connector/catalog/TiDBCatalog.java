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

package io.tidb.bigdata.flink.connector.catalog;

import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory;
import io.tidb.bigdata.flink.tidb.TiDBBaseCatalog;
import io.tidb.bigdata.flink.tidb.TypeUtils;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
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

  @Override
  public TableSchema getTableSchema(String databaseName, String tableName) {
    Builder schemaBuilder = getClientSession()
        .getTableMust(databaseName, tableName)
        .getColumns()
        .stream()
        .reduce(TableSchema.builder(), (builder, c) -> builder.field(c.getName(),
            TypeUtils.getFlinkType(c.getType())), (builder1, builder2) -> null);
    TiDBSchemaAdapter.parserMetadataColumns(properties)
        .forEach((name, metadata) -> schemaBuilder.field(name, metadata.getType()));
    return schemaBuilder.build();
  }
}
