/*
 * Copyright 2020 TiKV Project Authors.
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

package org.tikv.bigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.bigdata.tidb.ClientConfig;

public abstract class TiDBBaseDynamicTableSource implements ScanTableSource {

  static final Logger LOG = LoggerFactory.getLogger(TiDBBaseDynamicTableSource.class);

  protected final TableSchema tableSchema;

  protected final Map<String, String> properties;

  protected final ClientConfig config;

  public TiDBBaseDynamicTableSource(TableSchema tableSchema, Map<String, String> properties) {
    this.tableSchema = tableSchema;
    this.properties = properties;
    this.config = new ClientConfig(properties);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }

  protected String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(properties.get(key), key + " can not be null");
  }

}
