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
package com.zhihu.presto.tidb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor;
import shade.com.google.protobuf.ByteString;

import javax.inject.Inject;

import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class ClientSession implements AutoCloseable
{
    private ClientConfig config;

    private final TiSession session;
    private Catalog catalog;

    @Inject
    public ClientSession(ClientConfig config)
    {
        this.config = requireNonNull(config, "config is null");

        session = TiSession.getInstance(TiConfiguration.createDefault(config.getPdAddresses()));
        catalog = session.getCatalog();
    }

    private static List<ColumnHandleInternal> getTableColumns(TiTableInfo table)
    {
        Stream<Integer> indexStream = Stream.iterate(0, i -> i + 1);
        return Streams.mapWithIndex(table.getColumns().stream(), (column, i) -> new ColumnHandleInternal(column.getName(), column.getType(), (int) i)).collect(toImmutableList());
    }

    public List<String> getSchemaNames()
    {
        return catalog.listDatabases().stream().map(TiDBInfo::getName).collect(toImmutableList());
    }

    public List<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        TiDBInfo db = catalog.getDatabase(schema);
        if (db == null) {
            return ImmutableList.of();
        }
        return catalog.listTables(db).stream().map(TiTableInfo::getName).collect(toImmutableList());
    }

    public Optional<TiTableInfo> getTable(TableHandleInternal handle)
    {
        return getTable(handle.getSchemaName(), handle.getTableName());
    }

    public Optional<TiTableInfo> getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return Optional.ofNullable(catalog.getTable(schema, tableName));
    }

    public TiTableInfo getTableMust(TableHandleInternal handle)
    {
        return getTableMust(handle.getSchemaName(), handle.getTableName());
    }

    public TiTableInfo getTableMust(String schema, String tableName)
    {
        return getTable(schema, tableName).orElseThrow(() -> new IllegalStateException("Table " + schema + "." + tableName + " no longer exists"));
    }

    public Map<String, List<String>> listTables(Optional<String> schemaName)
    {
        List<String> schemaNames = schemaName
                .map(s -> (List<String>) ImmutableList.of(s))
                .orElseGet(() -> getSchemaNames());
        return schemaNames.stream().collect(toImmutableMap(identity(), name -> getTableNames(name)));
    }

    public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName)
    {
        return getTable(schema, tableName).map(ClientSession::getTableColumns);
    }

    public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName, List<String> columns)
    {
        Set<String> columnsSet = columns.stream().collect(toImmutableSet());
        return getTableColumns(schema, tableName).map(r -> r.stream().filter(column -> columnsSet.contains(column.getName())).collect(toImmutableList()));
    }

    public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle)
    {
        return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle, List<String> columns)
    {
        return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName(), columns);
    }

    private List<RangeSplitter.RegionTask> getRangeRegionTasks(ByteString startKey, ByteString endKey)
    {
        List<Coprocessor.KeyRange> keyRanges =
                ImmutableList.of(KeyRangeUtils.makeCoprocRange(startKey, endKey));
        return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(keyRanges);
    }

    private List<RangeSplitter.RegionTask> getRangeRegionTasks(Base64KeyRange range)
    {
        ByteString startKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getStartKey()));
        ByteString endKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getEndKey()));
        return getRangeRegionTasks(startKey, endKey);
    }

    private List<RangeSplitter.RegionTask> getTableRegionTasks(TableHandleInternal tableHandle)
    {
        return getTable(tableHandle).map(table -> {
            long tableId = table.getId();
            RowKey start = RowKey.createMin(tableId);
            RowKey end = RowKey.createBeyondMax(tableId);
            return getRangeRegionTasks(start.toByteString(), end.toByteString());
        }).orElseGet(ImmutableList::of);
    }

    public List<Base64KeyRange> getTableRanges(TableHandleInternal tableHandle)
    {
        Base64.Encoder encoder = Base64.getEncoder();
        return getTableRegionTasks(tableHandle).stream().flatMap(task -> task.getRanges().stream().map(range -> {
            String taskStart = encoder.encodeToString(range.getStart().toByteArray());
            String taskEnd = encoder.encodeToString(range.getEnd().toByteArray());
            return new Base64KeyRange(taskStart, taskEnd);
        })).collect(toImmutableList());
    }

    public TiDAGRequest.Builder request(TableHandleInternal table, List<String> columns)
    {
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

    public CoprocessIterator<Row> iterate(TiDAGRequest.Builder request, Base64KeyRange range)
    {
        return CoprocessIterator.getRowIterator(request.build(TiDAGRequest.PushDownType.NORMAL), getRangeRegionTasks(range), session);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("config", config)
                .toString();
    }

    @Override
    public void close() throws Exception
    {
        session.close();
    }
}
