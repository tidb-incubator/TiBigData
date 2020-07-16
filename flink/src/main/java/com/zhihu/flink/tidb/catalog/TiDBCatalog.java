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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
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

  private final Properties properties;

  private Optional<ClientSession> clientSession = Optional.empty();

  private Optional<Connection> connection = Optional.empty();

  private Optional<Statement> statement = Optional.empty();

  public TiDBCatalog(Properties properties) {
    super(DEFAULT_NAME, DEFAULT_DATABASE);
    this.properties = Preconditions.checkNotNull(properties);
    this.pdAddresses = Preconditions.checkNotNull(properties.getProperty(TiDBOptions.PD_ADDRESSES));
  }

  @Override
  public void open() throws CatalogException {
    // catalog isOpened?
    if (!clientSession.isPresent()) {
      try {
        clientSession = Optional.of(new ClientSession(new ClientConfig(pdAddresses)));
        String dbUrl = properties.getProperty(TiDBOptions.DATABASE_URL);
        if (dbUrl != null) {
          Class.forName(TiDBOptions.DRIVER_NAME);
          String username = Preconditions
              .checkNotNull(properties.getProperty(TiDBOptions.USERNAME));
          String password = properties.getProperty(TiDBOptions.PASSWORD);
          connection = Optional.of(DriverManager.getConnection(dbUrl, username, password));
          statement = Optional.of(connection.get().createStatement());
        }
      } catch (Exception e) {
        throw new CatalogException("can not open catalog", e);
      }
    }
  }

  @Override
  public void close() throws CatalogException {
    clientSession.ifPresent(session -> {
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("can not close clientSession", e);
      }
      clientSession = Optional.empty();
    });
    statement.ifPresent(stat -> {
      try {
        stat.close();
      } catch (SQLException e) {
        LOG.warn("can not close statement", e);
      }
      statement = Optional.empty();
    });
    connection.ifPresent(con -> {
      try {
        con.close();
      } catch (SQLException e) {
        LOG.warn("can not close connection", e);
      }
      connection = Optional.empty();
    });
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

  public void createDatabase(String databaseName, boolean ignoreIfExists) {
    Preconditions.checkNotNull(databaseName);
    sqlUpdate(String
        .format("CREATE DATABASE %s `%s`", ignoreIfExists ? "IF NOT EXISTS" : "", databaseName));
  }

  @Override
  public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    Preconditions.checkNotNull(databaseName);
    sqlUpdate(String
        .format("DROP DATABASE %s `%s`", ignoreIfNotExists ? "IF EXISTS" : "", databaseName));
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    Preconditions.checkNotNull(databaseName);
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
    Preconditions.checkNotNull(tablePath);
    return getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public CatalogBaseTable getTable(String databaseName, String tableName)
      throws TableNotExistException, CatalogException {
    return new CatalogTableImpl(getTableSchema(databaseName, tableName), ImmutableMap.of(), "");
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    Preconditions.checkNotNull(tablePath);
    return tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  public boolean tableExists(String databaseName, String tableName) throws CatalogException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    return databaseExists(databaseName) && getClientSession().getTableNames(databaseName)
        .contains(tableName);
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    Preconditions.checkNotNull(tablePath);
    dropTable(tablePath.getDatabaseName(), tablePath.getObjectName(), ignoreIfNotExists);
  }

  public void dropTable(String databaseName, String tableName, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    sqlUpdate(String
        .format("DROP TABLE %s `%s`.`%s`", ignoreIfNotExists ? "IF EXISTS" : "", databaseName,
            tableName));
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

  public synchronized void sqlUpdate(String sql) {
    try {
      statement.orElseThrow(IllegalStateException::new).executeUpdate(sql);
    } catch (SQLException e) {
      throw new CatalogException(e);
    }
  }

  private ClientSession getClientSession() {
    return clientSession.orElseThrow(IllegalStateException::new);
  }

  public static class TiDBOptions {

    public static final String PD_ADDRESSES = "pd.addresses";

    public static final String DATABASE_URL = "database.url";

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
  }
}
