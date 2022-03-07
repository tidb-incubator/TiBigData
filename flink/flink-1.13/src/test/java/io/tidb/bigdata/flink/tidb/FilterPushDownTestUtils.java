/*
 * Copyright 2022 TiDB Project Authors.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;

public class FilterPushDownTestUtils {

  private static final TableEnvironment tableEnvironment;

  static {
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner().inBatchMode().build();
    tableEnvironment = TableEnvironment.create(settings);
    TestCatalog testCatalog = new TestCatalog();
    tableEnvironment.registerCatalog(testCatalog.getName(), testCatalog);
  }

  private static String getSql(String whereConditions) {
    return String.format("SELECT * FROM `test_catalog`.`default`.`test` WHERE %s", whereConditions);
  }

  /**
   * We use sql to generate flink filters rather than flink expression api, which is closer to the
   * production environment and more intuitive.
   *
   * @param whereConditions Where conditions for sql, like '`c1` = 1'.
   * @return Flink filters converted from sql.
   */
  public static synchronized List<ResolvedExpression> getFilters(String whereConditions) {
    TestTableSource.FILTERS = null;
    tableEnvironment.executeSql(getSql(whereConditions));
    return TestTableSource.FILTERS;
  }

  public static class TestCatalog extends AbstractCatalog {

    public static final List<String> DATABASES = ImmutableList.of("default");
    public static final List<String> TABLES = ImmutableList.of("test");
    /**
     * All types for tidb: {@link io.tidb.bigdata.test.TableUtils#getTableSqlWithAllTypes(String,
     * String)}
     */
    public static final Schema SCHEMA = Schema.newBuilder()
        .column("c1", DataTypes.TINYINT())
        .column("c2", DataTypes.SMALLINT())
        .column("c3", DataTypes.INT())
        .column("c4", DataTypes.INT())
        .column("c5", DataTypes.BIGINT())
        .column("c6", DataTypes.STRING())
        .column("c7", DataTypes.STRING())
        .column("c8", DataTypes.STRING())
        .column("c9", DataTypes.STRING())
        .column("c10", DataTypes.STRING())
        .column("c11", DataTypes.STRING())
        .column("c12", DataTypes.BYTES())
        .column("c13", DataTypes.BYTES())
        .column("c14", DataTypes.BYTES())
        .column("c15", DataTypes.BYTES())
        .column("c16", DataTypes.BYTES())
        .column("c17", DataTypes.BYTES())
        .column("c18", DataTypes.FLOAT())
        .column("c19", DataTypes.DOUBLE())
        .column("c20", DataTypes.DECIMAL(6, 3))
        .column("c21", DataTypes.DATE())
        .column("c22", DataTypes.TIME(0))
        .column("c23", DataTypes.TIMESTAMP(6))
        .column("c24", DataTypes.TIMESTAMP(6))
        .column("c25", DataTypes.INT())
        .column("c26", DataTypes.TINYINT())
        .column("c27", DataTypes.STRING())
        .column("c28", DataTypes.STRING())
        .column("c29", DataTypes.STRING())
        .build();


    public TestCatalog() {
      this("test_catalog", "default");
    }

    public TestCatalog(String name, String defaultDatabase) {
      super(name, defaultDatabase);
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public List<String> listDatabases() throws CatalogException {
      return DATABASES;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      return new CatalogDatabaseImpl(ImmutableMap.of(), "");
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
      return DATABASES.contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
        throws DatabaseAlreadyExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
        throws DatabaseNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      return TABLES;
    }

    @Override
    public List<String> listViews(String databaseName)
        throws DatabaseNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      return CatalogTable.of(SCHEMA, "", ImmutableList.of(), ImmutableMap.of());
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
      return DATABASES.contains(tablePath.getDatabaseName()) && TABLES.contains(
          tablePath.getObjectName());
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
        throws TableNotExistException, TableAlreadyExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
        throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable,
        boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
        List<Expression> filters)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition partition, boolean ignoreIfExists)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition newPartition, boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String dbName)
        throws DatabaseNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
        throws FunctionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function,
        boolean ignoreIfExists)
        throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
        boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
        throws FunctionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
      return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
        boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath,
        CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException, TablePartitionedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Factory> getFactory() {
      return Optional.of(new TestFactory());
    }
  }

  public static class TestFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "test";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
      return new TestTableSource();
    }

    @Override
    public String factoryIdentifier() {
      return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
      return ImmutableSet.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
      return ImmutableSet.of();
    }
  }

  public static class TestTableSource implements ScanTableSource, SupportsProjectionPushDown,
      SupportsFilterPushDown {

    public static List<ResolvedExpression> FILTERS;

    @Override
    public DynamicTableSource copy() {
      return new TestTableSource();
    }

    @Override
    public String asSummaryString() {
      return "";
    }


    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
      return InputFormatProvider.of(new TestInputFormat());
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
      FILTERS = filters;
      return Result.of(Collections.emptyList(), filters);
    }

    @Override
    public boolean supportsNestedProjection() {
      return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {

    }
  }

  public static class TestInputFormat extends RichInputFormat<RowData, InputSplit> {

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
      return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
      return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
      return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
      return true;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
      return null;
    }

    @Override
    public void close() throws IOException {

    }
  }


}
