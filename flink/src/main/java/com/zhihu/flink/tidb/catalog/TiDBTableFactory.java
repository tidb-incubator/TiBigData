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

package com.zhihu.flink.tidb.catalog;

import com.zhihu.flink.tidb.source.TiDBTableSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

public class TiDBTableFactory implements TableSourceFactory<Row> {

  private final String pdAddresses;

  public TiDBTableFactory(String pdAddresses) {
    this.pdAddresses = pdAddresses;
  }

  @Override
  public TableSource<Row> createTableSource(ObjectPath tablePath, CatalogTable table) {
    Preconditions.checkNotNull(table);
    Preconditions.checkArgument(table instanceof CatalogTableImpl);
    return TiDBTableSource.builder()
        .setPdAddresses(pdAddresses)
        .setDatabaseName(tablePath.getDatabaseName())
        .setTableName(tablePath.getObjectName())
        .setTableSchema(table.getSchema())
        .build();
  }

  @Override
  public TableSource<Row> createTableSource(Map<String, String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> requiredContext() {
    return new HashMap<>();
  }

  @Override
  public List<String> supportedProperties() {
    return new ArrayList<>();
  }
}
