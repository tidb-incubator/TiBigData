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

import io.tidb.bigdata.flink.connector.source.TiDBMetadata;
import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.source.TiDBSourceBuilder;
import io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper;
import io.tidb.bigdata.flink.connector.utils.LookupTableSourceHelper;
import io.tidb.bigdata.flink.connector.utils.StoreVersion;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import  io.tidb.bigdata.tidb.expression.Expression;
import org.tikv.common.meta.TiTableInfo;

public class TiDBDynamicTableSource implements ScanTableSource, LookupTableSource,
    SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown,
    SupportsReadingMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBDynamicTableSource.class);

  private final ResolvedCatalogTable table;
  private final ChangelogMode changelogMode;
  private final LookupTableSourceHelper lookupTableSourceHelper;
  private TiDBSchemaAdapter schema;
  private FilterPushDownHelper filterPushDownHelper;
  private Integer limit;
  private Expression expression;

  public TiDBDynamicTableSource(ResolvedCatalogTable table,
      ChangelogMode changelogMode, JdbcLookupOptions lookupOptions) {
    this(table, changelogMode, new LookupTableSourceHelper(lookupOptions));
  }

  private TiDBDynamicTableSource(ResolvedCatalogTable table,
      ChangelogMode changelogMode, LookupTableSourceHelper lookupTableSourceHelper) {
    this.table = table;
    this.changelogMode = changelogMode;
    this.lookupTableSourceHelper = lookupTableSourceHelper;
    this.schema = new TiDBSchemaAdapter(table);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return changelogMode;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return SourceProvider.of(new TiDBSourceBuilder(table, schema, expression, limit).build());
  }

  @Override
  public DynamicTableSource copy() {
    TiDBDynamicTableSource otherSource =
        new TiDBDynamicTableSource(table, changelogMode, lookupTableSourceHelper);
    otherSource.filterPushDownHelper = this.filterPushDownHelper;
    otherSource.schema = this.schema;
    return otherSource;
  }

  @Override
  public String asSummaryString() {
    return "";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return lookupTableSourceHelper.getLookupRuntimeProvider(table, context);
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    schema.applyProjectedFields(Arrays.stream(projectedFields).mapToInt(f -> f[0]).toArray());
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    ClientConfig clientConfig = new ClientConfig(table.getOptions());
    if (clientConfig.isFilterPushDown() && filterPushDownHelper == null) {
      String databaseName = getRequiredProperties(TiDBOptions.DATABASE_NAME.key());
      String tableName = getRequiredProperties(TiDBOptions.TABLE_NAME.key());
      TiTableInfo tiTableInfo;
      try (ClientSession clientSession = ClientSession.create(clientConfig)) {
        tiTableInfo = clientSession.getTableMust(databaseName, tableName);

        List<StoreVersion> tiKVVersions = StoreVersion.fetchTiKVVersions(
            clientSession.getTiSession().getPDClient());
        this.filterPushDownHelper = new FilterPushDownHelper(tiTableInfo, tiKVVersions);
      } catch (Exception e) {
        throw new IllegalStateException("can not get table", e);
      }
      LOG.info("Flink filters: " + filters);
      this.expression = filterPushDownHelper.toTiDBExpression(filters).orElse(null);
      LOG.info("TiDB filters: " + expression);
    }
    return Result.of(Collections.emptyList(), filters);
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(table.getOptions().get(key), key + " can not be null");
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit;
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    return Arrays.stream(TiDBMetadata.values())
        .collect(Collectors.toMap(TiDBMetadata::getKey, TiDBMetadata::getType));
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    schema.applyReadableMetadata(metadataKeys, producedDataType);
  }
}