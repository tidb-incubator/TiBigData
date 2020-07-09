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

import com.google.common.collect.ImmutableMap;
import com.zhihu.flink.tidb.utils.DataTypeMappingUtil;
import com.zhihu.presto.tidb.ClientConfig;
import com.zhihu.presto.tidb.ClientSession;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
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
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBCatalog extends AbstractCatalog {

  static final Logger LOG = LoggerFactory.getLogger(TiDBCatalog.class);

  public static final String DEFAULT_DATABASE = "default";

  public static final String DEFAULT_NAME = "tidb";

  private final String pdAddresses;

  private Optional<ClientSession> clientSession = Optional.empty();

  private boolean isOpened;

  public TiDBCatalog(String pdAddresses) {
    this(DEFAULT_NAME, DEFAULT_DATABASE, pdAddresses);
  }

  public TiDBCatalog(String name, String pdAddresses) {
    this(name, DEFAULT_DATABASE, pdAddresses);
  }

  public TiDBCatalog(String name, String defaultDatabase, String pdAddresses) {
    super(name, defaultDatabase);
    this.pdAddresses = pdAddresses;
  }

  @Override
  public void open() throws CatalogException {
    if (!isOpened) {
      clientSession = Optional.of(new ClientSession(new ClientConfig(pdAddresses)));
    }
    isOpened = true;
  }

  @Override
  public void close() throws CatalogException {
    if (isOpened) {
      clientSession.ifPresent(clientSession -> {
        try {
          clientSession.close();
        } catch (Exception e) {
          LOG.warn("can not close clientSession", e);
        }
      });
      isOpened = false;
    }
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return getClientSession().getSchemaNames();
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    return new CatalogDatabaseImpl(ImmutableMap.of(), "");
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    Preconditions.checkNotNull(databaseName);
    return getClientSession().getSchemaNames().contains(databaseName);
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
    return getClientSession().getTableNames(databaseName);
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public CatalogBaseTable getTable(String databaseName, String tableName)
      throws TableNotExistException, CatalogException {
    return new CatalogTableImpl(getTableSchema(databaseName, tableName), ImmutableMap.of(), "");
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public boolean tableExists(String databaseName, String tableName) throws CatalogException {
    return databaseExists(databaseName) && getClientSession().getTableNames(databaseName)
        .contains(tableName);
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
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
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
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
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
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
      PartitionAlreadyExistsException, CatalogException {
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
    return null;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    return null;
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

  public TableSchema getTableSchema(String databaseName, String tableName) {
    return getClientSession()
        .getTableColumns(databaseName, tableName)
        .orElseThrow(() -> new NullPointerException("can not get table columns"))
        .stream()
        .reduce(TableSchema.builder(), (builder, c) -> builder.field(c.getName(),
            DataTypeMappingUtil.mapToFlinkType(c.getType())), (builder1, builder2) -> null).build();
  }

  @Override
  public Optional<TableFactory> getTableFactory() {
    return Optional.of(new TiDBTableFactory(pdAddresses));
  }

  private ClientSession getClientSession() {
    return clientSession.orElseThrow(IllegalStateException::new);
  }
}
