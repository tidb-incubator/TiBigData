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

package io.tidb.bigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.tidb.bigdata.tidb.SqlUtils.QUERY_PD_SQL;
import static io.tidb.bigdata.tidb.SqlUtils.getCreateTableSql;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiDAGRequest;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiPartitionDef;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.operation.iterator.CoprocessorIterator;
import org.tikv.common.row.Row;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

public final class ClientSession implements AutoCloseable {

  static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

  private final ClientConfig config;

  private final TiSession session;

  private final Catalog catalog;

  private final HikariDataSource dataSource;

  private final DnsSearchHostMapping hostMapping;

  private ClientSession(ClientConfig config) {
    this.config = requireNonNull(config, "config is null");
    dataSource = new HikariDataSource(new HikariConfig() {
      {
        setJdbcUrl(requireNonNull(config.getDatabaseUrl(), "database url can not be null"));
        setUsername(requireNonNull(config.getUsername(), "username can not be null"));
        setPassword(config.getPassword());
        setDriverClassName(config.getDriverName());
        setMaximumPoolSize(config.getMaximumPoolSize());
        setMinimumIdle(config.getMinimumIdleSize());
      }
    });
    hostMapping = new DnsSearchHostMapping(config.getDnsSearch());
    loadPdAddresses();
    TiConfiguration tiConfiguration = TiConfiguration.createDefault(config.getPdAddresses());
    ReplicaReadPolicy policy = config.getReplicaReadPolicy();
    tiConfiguration.setReplicaRead(policy.toReplicaRead());
    tiConfiguration.setReplicaSelector(policy);
    tiConfiguration.setHostMapping(hostMapping);
    session = TiSession.create(tiConfiguration);
    catalog = session.getCatalog();
  }

  public List<String> getSchemaNames() {
    return catalog.listDatabases().stream().map(TiDBInfo::getName).collect(toImmutableList());
  }

  public List<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    TiDBInfo db = catalog.getDatabase(schema);
    if (db == null) {
      return ImmutableList.of();
    }
    return catalog.listTables(db).stream().map(TiTableInfo::getName).collect(toImmutableList());
  }

  public Optional<TiTableInfo> getTable(TableHandleInternal handle) {
    return getTable(handle.getSchemaName(), handle.getTableName());
  }

  public Optional<TiTableInfo> getTable(String schema, String tableName) {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");
    return Optional.ofNullable(catalog.getTable(schema, tableName));
  }

  public TiTableInfo getTableMust(TableHandleInternal handle) {
    return getTableMust(handle.getSchemaName(), handle.getTableName());
  }

  public TiTableInfo getTableMust(String schema, String tableName) {
    return getTable(schema, tableName).orElseThrow(
        () -> new IllegalStateException("Table " + schema + "." + tableName + " no longer exists"));
  }

  public Map<String, List<String>> listTables(Optional<String> schemaName) {
    List<String> schemaNames = schemaName
        .map(s -> (List<String>) ImmutableList.of(s))
        .orElseGet(this::getSchemaNames);
    return schemaNames.stream().collect(toImmutableMap(identity(), this::getTableNames));
  }

  private List<ColumnHandleInternal> selectColumns(
      List<ColumnHandleInternal> allColumns, Stream<String> columns) {
    final Map<String, ColumnHandleInternal> columnsMap =
        allColumns.stream().collect(
            Collectors.toMap(ColumnHandleInternal::getName, Function.identity()));
    return columns.map(columnsMap::get).collect(Collectors.toList());
  }

  private static List<ColumnHandleInternal> getTableColumns(TiTableInfo table) {
    return Streams.mapWithIndex(table.getColumns().stream(),
        (column, i) -> new ColumnHandleInternal(column.getName(), column.getType(), (int) i))
        .collect(toImmutableList());
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName) {
    return getTable(schema, tableName).map(ClientSession::getTableColumns);
  }

  private Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
      Stream<String> columns) {
    return getTableColumns(schema, tableName).map(r -> selectColumns(r, columns));
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
      List<String> columns) {
    return getTableColumns(schema, tableName, columns.stream());
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
      String[] columns) {
    return getTableColumns(schema, tableName, Arrays.stream(columns));
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle) {
    return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName());
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle,
      List<String> columns) {
    return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName(), columns);
  }

  private List<RangeSplitter.RegionTask> getRangeRegionTasks(ByteString startKey,
      ByteString endKey) {
    List<Coprocessor.KeyRange> keyRanges =
        ImmutableList.of(KeyRangeUtils.makeCoprocRange(startKey, endKey));
    return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(keyRanges);
  }

  private List<RangeSplitter.RegionTask> getRangeRegionTasks(Base64KeyRange range) {
    ByteString startKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getStartKey()));
    ByteString endKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getEndKey()));
    return getRangeRegionTasks(startKey, endKey);
  }

  private List<RangeSplitter.RegionTask> getTableRegionTasks(TableHandleInternal tableHandle) {
    return getTable(tableHandle)
        .map(table -> table.isPartitionEnabled()
            ? table.getPartitionInfo().getDefs().stream().map(TiPartitionDef::getId)
            .collect(Collectors.toList()) : ImmutableList.of(table.getId()))
        .orElseGet(ImmutableList::of)
        .stream()
        .map(tableId -> getRangeRegionTasks(RowKey.createMin(tableId).toByteString(),
            RowKey.createBeyondMax(tableId).toByteString()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<Base64KeyRange> getTableRanges(TableHandleInternal tableHandle) {
    Base64.Encoder encoder = Base64.getEncoder();
    return getTableRegionTasks(tableHandle).stream()
        .flatMap(task -> task.getRanges().stream().map(range -> {
          String taskStart = encoder.encodeToString(range.getStart().toByteArray());
          String taskEnd = encoder.encodeToString(range.getEnd().toByteArray());
          return new Base64KeyRange(taskStart, taskEnd);
        })).collect(toImmutableList());
  }

  public TiDAGRequest.Builder request(TableHandleInternal table, List<String> columns) {
    TiTableInfo tableInfo = getTableMust(table);
    if (columns.isEmpty()) {
      columns = ImmutableList.of(tableInfo.getColumns().get(0).getName());
    }
    return TiDAGRequest.Builder
        .newBuilder()
        .setFullTableScan(tableInfo)
        .addRequiredCols(columns)
        .setStartTs(session.getTimestamp());
  }

  public CoprocessorIterator<Row> iterate(TiDAGRequest.Builder request, Base64KeyRange range) {
    return CoprocessorIterator
        .getRowIterator(request.build(TiDAGRequest.PushDownType.NORMAL), getRangeRegionTasks(range),
            session);
  }

  private void loadPdAddresses() {
    if (config.getPdAddresses() == null) {
      List<String> pdAddressesList = new ArrayList<>();
      try (
          Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(QUERY_PD_SQL)
      ) {
        while (resultSet.next()) {
          String instance = resultSet.getString("INSTANCE");
          URI mapped = hostMapping.getMappedURI(URI.create("grpc://" + instance));
          pdAddressesList.add(mapped.getHost() + ":" + mapped.getPort());
        }
      } catch (Exception e) {
        throw new IllegalStateException("can not get pdAddresses", e);
      }
      config.setPdAddresses(String.join(",", pdAddressesList));
    }
  }

  public void sqlUpdate(String... sqls) {
    try (
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()
    ) {
      for (String sql : sqls) {
        LOG.info("sql update: " + sql);
        statement.executeUpdate(sql);
      }
    } catch (Exception e) {
      LOG.error("execute sql fail", e);
      throw new IllegalStateException(e);
    }
  }

  public void createTable(String databaseName, String tableName, List<String> columnNames,
      List<String> columnTypes, List<String> primaryKeyColumns, List<String> uniqueKeyColumns,
      boolean ignoreIfExists) {
    sqlUpdate(getCreateTableSql(requireNonNull(databaseName), requireNonNull(tableName),
        requireNonNull(columnNames), requireNonNull(columnTypes), primaryKeyColumns,
        uniqueKeyColumns, ignoreIfExists));
  }

  public void dropTable(String databaseName, String tableName, boolean ignoreIfNotExists) {
    sqlUpdate(String.format("DROP TABLE %s `%s`.`%s`", ignoreIfNotExists ? "IF EXISTS" : "",
        requireNonNull(databaseName), requireNonNull(tableName)));
  }

  public void createDatabase(String databaseName, boolean ignoreIfExists) {
    sqlUpdate(String.format("CREATE DATABASE %s `%s`", ignoreIfExists ? "IF NOT EXISTS" : "",
        requireNonNull(databaseName)));
  }

  public void dropDatabase(String databaseName, boolean ignoreIfNotExists) {
    sqlUpdate(String.format("DROP DATABASE %s `%s`", ignoreIfNotExists ? "IF EXISTS" : "",
        requireNonNull(databaseName)));
  }

  public boolean databaseExists(String databaseName) {
    return getSchemaNames().contains(requireNonNull(databaseName));
  }

  public boolean tableExists(String databaseName, String tableName) {
    return databaseExists(requireNonNull(databaseName))
        && getTableNames(databaseName).contains(requireNonNull(tableName));
  }

  public void renameTable(String oldDatabaseName, String newDatabaseName, String oldTableName,
      String newTableName) {
    sqlUpdate(String.format("RENAME TABLE `%s`.`%s` TO `%s`.`%s` ",
        requireNonNull(oldDatabaseName),
        requireNonNull(oldTableName),
        requireNonNull(newDatabaseName),
        requireNonNull(newTableName)));
  }

  public void addColumn(String databaseName, String tableName, String columnName,
      String columnType) {
    sqlUpdate(String.format("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s",
        requireNonNull(databaseName),
        requireNonNull(tableName),
        requireNonNull(columnName),
        requireNonNull(columnType)));
  }

  public void renameColumn(String databaseName, String tableName, String oldName, String newName,
      String newType) {
    sqlUpdate(String.format("ALTER TABLE `%s`.`%s` CHANGE `%s` `%s` %s",
        requireNonNull(databaseName),
        requireNonNull(tableName),
        requireNonNull(oldName),
        requireNonNull(newName),
        requireNonNull(newType)));
  }

  public void dropColumn(String databaseName, String tableName, String columnName) {
    sqlUpdate(String.format("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`",
        requireNonNull(databaseName),
        requireNonNull(tableName),
        requireNonNull(columnName)));
  }

  public Connection getJdbcConnection() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("config", config)
        .toString();
  }

  public List<String> getPrimaryKeyColumns(String databaseName, String tableName) {
    return getTableMust(databaseName, tableName).getColumns().stream()
        .filter(TiColumnInfo::isPrimaryKey).map(TiColumnInfo::getName).collect(Collectors.toList());
  }

  public List<String> getUniqueKeyColumns(String databaseName, String tableName) {
    List<String> primaryKeyColumns = getPrimaryKeyColumns(databaseName, tableName);
    return getTableMust(databaseName, tableName).getIndices().stream()
        .filter(TiIndexInfo::isUnique)
        .map(TiIndexInfo::getIndexColumns)
        .flatMap(Collection::stream).map(TiIndexColumn::getName)
        .filter(name -> !primaryKeyColumns.contains(name)).collect(Collectors.toList());
  }

  public TiTimestamp getTimestamp() {
    return session.getTimestamp();
  }

  @Override
  public synchronized void close() throws Exception {
    session.close();
    dataSource.close();
  }

  public TiTimestamp getSnapshotVersion() {
    return session.getTimestamp();
  }

  public static ClientSession createWithSingleConnection(ClientConfig config) {
    ClientConfig clientConfig = new ClientConfig(config);
    clientConfig.setMaximumPoolSize(1);
    clientConfig.setMinimumIdleSize(1);
    return new ClientSession(clientConfig);
  }

  public static ClientSession create(ClientConfig config) {
    return new ClientSession(new ClientConfig(config));
  }
}
