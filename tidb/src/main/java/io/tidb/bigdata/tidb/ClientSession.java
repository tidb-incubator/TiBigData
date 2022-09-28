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
import static io.tidb.bigdata.tidb.SqlUtils.QUERY_CLUSTERED_INDEX_SQL_FORMAT;
import static io.tidb.bigdata.tidb.SqlUtils.QUERY_PD_SQL;
import static io.tidb.bigdata.tidb.SqlUtils.TIDB_ROW_FORMAT_VERSION_SQL;
import static io.tidb.bigdata.tidb.SqlUtils.getCreateTableSql;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.JdbcConnectionProvider;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator.RowIDAllocatorType;
import io.tidb.bigdata.tidb.allocator.RowIDAllocator;
import io.tidb.bigdata.tidb.catalog.Catalog;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.TableHandleInternal;
import io.tidb.bigdata.tidb.key.Base64KeyRange;
import io.tidb.bigdata.tidb.key.RowKey;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiDAGRequest;
import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiPartitionDef;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.Row;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.Pair;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

public final class ClientSession implements AutoCloseable {

  public static final String TIDB_ROW_ID_COLUMN_NAME = "_tidb_rowid";

  private static final Set<String> BUILD_IN_DATABASES =
      ImmutableSet.of("information_schema", "metrics_schema", "performance_schema", "mysql");

  static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);
  private static final String TIDB_ENABLE_CLUSTERED_INDEX = "select @@tidb_enable_clustered_index";

  private final ClientConfig config;

  private final TiSession session;

  private final Catalog catalog;

  private final JdbcConnectionProvider jdbcConnectionProvider;

  private final DnsSearchHostMapping hostMapping;

  private ClientSession(ClientConfig config) {
    this.config = requireNonNull(config, "config is null");
    this.jdbcConnectionProvider =
        JdbcConnectionProviderFactory.createJdbcConnectionProvider(config);
    hostMapping = new DnsSearchHostMapping(config.getDnsSearch());
    loadPdAddresses();
    TiConfiguration tiConfiguration = TiConfiguration.createDefault(config.getPdAddresses());

    tiConfiguration.setTlsEnable(config.getClusterTlsEnabled());
    tiConfiguration.setTrustCertCollectionFile(config.getClusterTlsCA());
    tiConfiguration.setKeyCertChainFile(config.getClusterTlsCert());
    tiConfiguration.setKeyFile(config.getClusterTlsKey());
    tiConfiguration.setJksEnable(config.getClusterUseJks());
    tiConfiguration.setJksKeyPath(config.getClusterJksKeyPath());
    tiConfiguration.setJksKeyPassword(config.getClusterJksKeyPassword());
    tiConfiguration.setJksTrustPath(config.getClusterJksTrustPath());
    tiConfiguration.setJksTrustPassword(config.getClusterJksTrustPassword());
    tiConfiguration.setTimeout(config.getTimeout());
    tiConfiguration.setScanTimeout(config.getScanTimeout());
    ReplicaReadPolicy policy = config.getReplicaReadPolicy();
    tiConfiguration.setReplicaSelector(policy);
    tiConfiguration.setHostMapping(hostMapping);
    session = TiSession.create(tiConfiguration);
    // TODO: enhancement to make catalog singleton
    catalog =
        new Catalog(
            session::createSnapshot, tiConfiguration.ifShowRowId(), tiConfiguration.getDBPrefix());
  }

  public List<String> getSchemaNames() {
    String sql = "SHOW DATABASES";
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> databaseNames = new ArrayList<>();
      while (resultSet.next()) {
        String databaseName = resultSet.getString(1).toLowerCase();
        if (BUILD_IN_DATABASES.contains(databaseName) && !config.isBuildInDatabaseVisible()) {
          continue;
        }
        databaseNames.add(databaseName);
      }
      return databaseNames;
    } catch (Exception e) {
      LOG.error("Execute sql {} fail", sql, e);
      throw new IllegalStateException(e);
    }
  }

  public List<String> getTableNames(String schema) {
    String sql = "SHOW TABLES";
    requireNonNull(schema, "schema is null");
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + schema);
      ResultSet resultSet = statement.executeQuery(sql);
      List<String> tableNames = new ArrayList<>();
      while (resultSet.next()) {
        tableNames.add(resultSet.getString(1).toLowerCase());
      }
      return tableNames;
    } catch (Exception e) {
      LOG.error("Execute sql {} fail", sql, e);
      throw new IllegalStateException(e);
    }
  }

  public Optional<TiTableInfo> getTable(String schema, String tableName) {
    return getTable(schema, tableName, false);
  }

  private Optional<TiTableInfo> getTable(String schema, String tableName, boolean showRowId) {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");
    return Optional.ofNullable(catalog.getTable(schema, tableName, showRowId));
  }

  public TiTableInfo getTableMust(String schema, String tableName) {
    return getTableMust(schema, tableName, false);
  }

  protected TiTableInfo getTableMust(String schema, String tableName, boolean showRowId) {
    return getTable(schema, tableName, showRowId)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("Table `%s`.`%s` no longer exists in TiDB", schema, tableName)));
  }

  public Map<String, List<String>> listTables(Optional<String> schemaName) {
    List<String> schemaNames =
        schemaName.map(s -> (List<String>) ImmutableList.of(s)).orElseGet(this::getSchemaNames);
    return schemaNames.stream().collect(toImmutableMap(identity(), this::getTableNames));
  }

  private List<ColumnHandleInternal> selectColumns(
      List<ColumnHandleInternal> allColumns, Stream<String> columns) {
    final Map<String, ColumnHandleInternal> columnsMap =
        allColumns.stream()
            .collect(Collectors.toMap(ColumnHandleInternal::getName, Function.identity()));
    return columns
        .map(
            column ->
                Objects.requireNonNull(
                    columnsMap.get(column), "Column `" + column + "` does not exist"))
        .collect(Collectors.toList());
  }

  private static List<ColumnHandleInternal> getTableColumns(TiTableInfo table) {
    return Streams.mapWithIndex(
            table.getColumns().stream(),
            (column, i) -> new ColumnHandleInternal(column.getName(), column.getType(), (int) i))
        .collect(toImmutableList());
  }

  private Optional<List<ColumnHandleInternal>> getTableColumns(
      String schema, String tableName, boolean showRowId) {
    return getTable(schema, tableName, showRowId).map(ClientSession::getTableColumns);
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName) {
    return getTableColumns(schema, tableName, false);
  }

  private Optional<List<ColumnHandleInternal>> getTableColumns(
      String schema, String tableName, Stream<String> columns) {
    List<String> columnList = columns.collect(Collectors.toList());
    boolean showRowId =
        columnList.stream().anyMatch(column -> column.equalsIgnoreCase(TIDB_ROW_ID_COLUMN_NAME));
    return getTableColumns(schema, tableName, showRowId)
        .map(r -> selectColumns(r, columnList.stream()));
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(
      String schema, String tableName, List<String> columns) {
    return getTableColumns(schema, tableName, columns.stream());
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(
      String schema, String tableName, String[] columns) {
    return getTableColumns(schema, tableName, Arrays.stream(columns));
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle) {
    return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName(), false);
  }

  public Optional<List<ColumnHandleInternal>> getTableColumns(
      TableHandleInternal tableHandle, List<String> columns) {
    return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName(), columns);
  }

  public List<ColumnHandleInternal> getTableColumnsMust(
      String schema, String tableName, boolean showRowId) {
    return getTableColumns(getTableMust(schema, tableName, showRowId));
  }

  public List<ColumnHandleInternal> getTableColumnsMust(String schema, String tableName) {
    return getTableColumnsMust(schema, tableName, false);
  }

  private List<RangeSplitter.RegionTask> getRangeRegionTasks(
      ByteString startKey, ByteString endKey) {
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
    return getTable(tableHandle.getSchemaName(), tableHandle.getTableName(), false)
        .map(
            table ->
                table.isPartitionEnabled()
                    ? table.getPartitionInfo().getDefs().stream()
                        .map(TiPartitionDef::getId)
                        .collect(Collectors.toList())
                    : ImmutableList.of(table.getId()))
        .orElseGet(ImmutableList::of).stream()
        .map(
            tableId ->
                getRangeRegionTasks(
                    RowKey.createMin(tableId).toByteString(),
                    RowKey.createBeyondMax(tableId).toByteString()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<Base64KeyRange> getTableRanges(TableHandleInternal tableHandle) {
    Base64.Encoder encoder = Base64.getEncoder();
    return getTableRegionTasks(tableHandle).stream()
        .flatMap(
            task ->
                task.getRanges().stream()
                    .map(
                        range -> {
                          String taskStart = encoder.encodeToString(range.getStart().toByteArray());
                          String taskEnd = encoder.encodeToString(range.getEnd().toByteArray());
                          return new Base64KeyRange(taskStart, taskEnd);
                        }))
        .collect(toImmutableList());
  }

  public TiDAGRequest.Builder request(TableHandleInternal table, List<String> columns) {
    boolean showRowId =
        columns.stream().anyMatch(column -> column.equalsIgnoreCase(TIDB_ROW_ID_COLUMN_NAME));
    TiTableInfo tableInfo = getTableMust(table.getSchemaName(), table.getTableName(), showRowId);
    if (columns.isEmpty()) {
      columns = ImmutableList.of(tableInfo.getColumns().get(0).getName());
    }
    return TiDAGRequest.Builder.newBuilder()
        .setFullTableScan(tableInfo)
        .addRequiredCols(columns)
        .setStartTs(session.getTimestamp());
  }

  public CoprocessorIterator<Row> iterate(TiDAGRequest.Builder request, Base64KeyRange range) {
    return iterate(request, ImmutableList.of(range));
  }

  public CoprocessorIterator<Row> iterate(
      TiDAGRequest.Builder request, List<Base64KeyRange> ranges) {
    List<RegionTask> regionTasks =
        ranges.stream()
            .map(this::getRangeRegionTasks)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    return CoprocessorIterator.getRowIterator(
        request.build(TiDAGRequest.PushDownType.NORMAL), regionTasks, session);
  }

  private void loadPdAddresses() {
    if (config.getPdAddresses() == null) {
      List<String> pdAddressesList = new ArrayList<>();
      try (Connection connection = jdbcConnectionProvider.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(QUERY_PD_SQL)) {
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
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        LOG.info("Sql update: " + sql);
        statement.executeUpdate(sql);
      }
    } catch (Exception e) {
      LOG.error("Execute sql fail", e);
      throw new IllegalStateException(e);
    }
  }

  public int queryTableCount(String databaseName, String tableName) {
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                format("SELECT COUNT(*) as c FROM `%s`.`%s`", databaseName, tableName))) {
      resultSet.next();
      return resultSet.getInt("c");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public int queryIndexCount(String databaseName, String tableName, String indexName) {
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                format(
                    "SELECT COUNT(`%s`) as c FROM `%s`.`%s`",
                    indexName, databaseName, tableName))) {
      resultSet.next();
      return resultSet.getInt("c");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public boolean isSupportClusteredIndex() {
    try (Connection connection = jdbcConnectionProvider.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(TIDB_ENABLE_CLUSTERED_INDEX)) {
      resultSet.next();
    } catch (SQLException e) {
      return false;
    }

    return true;
  }

  public void createTable(
      String databaseName,
      String tableName,
      List<String> columnNames,
      List<String> columnTypes,
      List<String> primaryKeyColumns,
      List<String> uniqueKeyColumns,
      boolean ignoreIfExists) {
    sqlUpdate(
        getCreateTableSql(
            requireNonNull(databaseName),
            requireNonNull(tableName),
            requireNonNull(columnNames),
            requireNonNull(columnTypes),
            primaryKeyColumns,
            uniqueKeyColumns,
            ignoreIfExists));
  }

  public void dropTable(String databaseName, String tableName, boolean ignoreIfNotExists) {
    sqlUpdate(
        String.format(
            "DROP TABLE %s `%s`.`%s`",
            ignoreIfNotExists ? "IF EXISTS" : "",
            requireNonNull(databaseName),
            requireNonNull(tableName)));
  }

  public void createDatabase(String databaseName, boolean ignoreIfExists) {
    sqlUpdate(
        String.format(
            "CREATE DATABASE %s `%s`",
            ignoreIfExists ? "IF NOT EXISTS" : "", requireNonNull(databaseName)));
  }

  public void dropDatabase(String databaseName, boolean ignoreIfNotExists) {
    sqlUpdate(
        String.format(
            "DROP DATABASE %s `%s`",
            ignoreIfNotExists ? "IF EXISTS" : "", requireNonNull(databaseName)));
  }

  public boolean databaseExists(String databaseName) {
    return getSchemaNames().contains(requireNonNull(databaseName));
  }

  public boolean tableExists(String databaseName, String tableName) {
    return databaseExists(requireNonNull(databaseName))
        && getTableNames(databaseName).contains(requireNonNull(tableName));
  }

  public void renameTable(
      String oldDatabaseName, String newDatabaseName, String oldTableName, String newTableName) {
    sqlUpdate(
        String.format(
            "RENAME TABLE `%s`.`%s` TO `%s`.`%s` ",
            requireNonNull(oldDatabaseName),
            requireNonNull(oldTableName),
            requireNonNull(newDatabaseName),
            requireNonNull(newTableName)));
  }

  public void addColumn(
      String databaseName, String tableName, String columnName, String columnType) {
    sqlUpdate(
        String.format(
            "ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s",
            requireNonNull(databaseName),
            requireNonNull(tableName),
            requireNonNull(columnName),
            requireNonNull(columnType)));
  }

  public void renameColumn(
      String databaseName, String tableName, String oldName, String newName, String newType) {
    sqlUpdate(
        String.format(
            "ALTER TABLE `%s`.`%s` CHANGE `%s` `%s` %s",
            requireNonNull(databaseName),
            requireNonNull(tableName),
            requireNonNull(oldName),
            requireNonNull(newName),
            requireNonNull(newType)));
  }

  public void dropColumn(String databaseName, String tableName, String columnName) {
    sqlUpdate(
        String.format(
            "ALTER TABLE `%s`.`%s` DROP COLUMN `%s`",
            requireNonNull(databaseName), requireNonNull(tableName), requireNonNull(columnName)));
  }

  public Connection getJdbcConnection() throws SQLException {
    return jdbcConnectionProvider.getConnection();
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("config", config).toString();
  }

  public List<String> getPrimaryKeyColumns(String databaseName, String tableName) {
    return getTableMust(databaseName, tableName).getColumns().stream()
        .filter(TiColumnInfo::isPrimaryKey)
        .map(TiColumnInfo::getName)
        .collect(Collectors.toList());
  }

  public List<List<String>> getUniqueKeyColumns(String databaseName, String tableName) {
    List<String> primaryKeyColumns = getPrimaryKeyColumns(databaseName, tableName);
    return getTableMust(databaseName, tableName).getIndices().stream()
        .filter(TiIndexInfo::isUnique)
        .map(TiIndexInfo::getIndexColumns)
        .map(list -> list.stream().map(TiIndexColumn::getName).collect(Collectors.toList()))
        .filter(list -> !list.equals(primaryKeyColumns))
        .collect(Collectors.toList());
  }

  public TiTimestamp getSnapshotVersion() {
    return session.getTimestamp();
  }

  public TiSession getTiSession() {
    return session;
  }

  public RowIDAllocator createRowIdAllocator(
      String databaseName, String tableName, long step, RowIDAllocatorType type) {
    long dbId = catalog.getDatabase(databaseName).getId();
    TiTableInfo table = getTableMust(databaseName, tableName);
    long shardBits = 0;
    boolean isUnsigned = false;
    switch (type) {
      case AUTO_INCREMENT:
        isUnsigned = table.isAutoIncrementColUnsigned();
        // AUTO_INC doesn't have shard bits.
        break;
      case AUTO_RANDOM:
        isUnsigned = table.isAutoRandomColUnsigned();
        shardBits = table.getAutoRandomBits();
        break;
      case IMPLICIT_ROWID:
        // IMPLICIT_ROWID is always signed.
        shardBits = table.getMaxShardRowIDBits();
        break;
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + type);
    }
    return RowIDAllocator.create(dbId, table, session, isUnsigned, step, shardBits, type);
  }

  public boolean isClusteredIndex(String databaseName, String tableName) {
    try (Connection connection = getJdbcConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("DESC `INFORMATION_SCHEMA`.`TABLES`")) {
        Set<String> fields = new HashSet<>();
        while (resultSet.next()) {
          fields.add(resultSet.getString("Field"));
        }
        if (!fields.contains("TIDB_PK_TYPE")) {
          return false;
        }
      }
      try (ResultSet resultSet =
          statement.executeQuery(
              String.format(QUERY_CLUSTERED_INDEX_SQL_FORMAT, databaseName, tableName))) {
        if (!resultSet.next()) {
          return false;
        }
        return resultSet.getString(1).equals("CLUSTERED");
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public int getRowFormatVersion() {
    try (Connection connection = getJdbcConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(TIDB_ROW_FORMAT_VERSION_SQL)) {
      if (!resultSet.next()) {
        return 1;
      }
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  // Please only use it for test
  public List<Pair<Row, Handle>> fetchAllRows(
      String databaseName, String tableName, TiTimestamp timestamp, boolean queryHandle) {
    List<SplitInternal> splits =
        new SplitManagerInternal(this)
            .getSplits(new TableHandleInternal("", databaseName, tableName));
    RecordSetInternal recordSetInternal =
        new RecordSetInternal(
            this,
            splits,
            getTableColumnsMust(databaseName, tableName),
            Optional.empty(),
            Optional.ofNullable(timestamp),
            Optional.empty(),
            queryHandle);
    RecordCursorInternal cursor = recordSetInternal.cursor();
    List<Pair<Row, Handle>> pairs = new ArrayList<>();
    while (cursor.advanceNextPosition()) {
      pairs.add(new Pair<>(cursor.getRow(), cursor.getHandle().orElse(null)));
    }
    return pairs;
  }

  // Please only use it for test
  public List<Pair<Row, Handle>> fetchAllRows(String databaseName, String tableName) {
    return fetchAllRows(databaseName, tableName, null, false);
  }

  @Override
  public synchronized void close() throws Exception {
    close(catalog, session, jdbcConnectionProvider);
  }

  private static void close(AutoCloseable... resources) {
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception e) {
        LOG.warn("Can not close resource", e);
      }
    }
  }

  public static ClientSession create(ClientConfig config) {
    return new ClientSession(new ClientConfig(config));
  }
}
