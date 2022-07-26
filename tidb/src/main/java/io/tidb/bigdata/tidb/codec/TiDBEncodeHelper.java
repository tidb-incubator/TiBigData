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

package io.tidb.bigdata.tidb.codec;

import com.google.common.base.Preconditions;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.handle.CommonHandle;
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.IntHandle;
import io.tidb.bigdata.tidb.key.IndexKey;
import io.tidb.bigdata.tidb.key.IndexKey.EncodeIndexDataResult;
import io.tidb.bigdata.tidb.key.RowKey;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.StoreVersion;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.Pair;

public class TiDBEncodeHelper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBEncodeHelper.class);

  public static final String VERSION = "4.0.0";
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] ZERO_BYTES = new byte[] {'0'};

  private final ClientSession session;
  private final TiTimestamp timestamp;
  private final String databaseName;
  private final String tableName;
  private final TiTableInfo tiTableInfo;
  private final Snapshot snapshot;
  private final List<TiColumnInfo> columns;
  private final List<String> columnNames;
  private final Optional<Integer> autoIncrementColumnIndex;
  private final List<TiIndexInfo> uniqueIndices;
  private final TiColumnInfo handleCol;
  private final boolean isCommonHandle;
  private final boolean enableNewRowFormat;
  private final boolean ignoreAutoincrementColumn;
  private final DynamicRowIDAllocator rowIdAllocator;
  private final boolean replace;

  public TiDBEncodeHelper(
      ClientSession session,
      TiTimestamp timestamp,
      String databaseName,
      String tableName,
      boolean ignoreAutoincrementColumn,
      boolean replace,
      DynamicRowIDAllocator rowIDAllocator) {
    this.session = session;
    this.timestamp = timestamp;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.ignoreAutoincrementColumn = ignoreAutoincrementColumn;
    this.tiTableInfo = session.getTableMust(databaseName, tableName);
    this.snapshot = session.getTiSession().createSnapshot(timestamp);
    this.columns = tiTableInfo.getColumns();
    this.columnNames = columns.stream().map(TiColumnInfo::getName).collect(Collectors.toList());
    this.autoIncrementColumnIndex =
        Optional.ofNullable(tiTableInfo.getAutoIncrementColInfo()).map(TiColumnInfo::getOffset);
    this.uniqueIndices =
        tiTableInfo.getIndices().stream()
            .filter(TiIndexInfo::isUnique)
            .collect(Collectors.toList());
    this.handleCol = tiTableInfo.getPKIsHandleColumn();
    this.isCommonHandle = session.isClusteredIndex(databaseName, tableName);
    if (StoreVersion.minTiKVVersion(VERSION, session.getTiSession().getPDClient())) {
      this.enableNewRowFormat = session.getRowFormatVersion() == 2;
    } else {
      this.enableNewRowFormat = false;
    }
    this.rowIdAllocator = rowIDAllocator;
    this.replace = replace;
  }

  private boolean isNullUniqueIndexValue(byte[] value) {
    return Arrays.equals(value, ZERO_BYTES);
  }

  private boolean isEmptyArray(byte[] array) {
    return array == null || array.length == 0;
  }

  private Handle extractHandle(Row tiRow) {
    if (tiTableInfo.isPkHandle()) {
      return new IntHandle(
          ((Number) tiRow.get(handleCol.getOffset(), handleCol.getType())).longValue());
    } else if (isCommonHandle) {
      List<DataType> dataTypes = new ArrayList<>();
      List<Object> data = new ArrayList<>();
      List<TiIndexColumn> indexColumns = new ArrayList<>();

      tiTableInfo
          .getPrimaryKey()
          .getIndexColumns()
          .forEach(
              indexColumn -> {
                TiColumnInfo column = tiTableInfo.getColumn(indexColumn.getName());
                dataTypes.add(0, column.getType());
                data.add(0, tiRow.get(column.getOffset(), column.getType()));
                indexColumns.add(0, indexColumn);
              });

      return CommonHandle.newCommonHandle(
          dataTypes.toArray(new DataType[0]),
          data.toArray(),
          indexColumns.stream().mapToLong(TiIndexColumn::getLength).toArray());
    } else {
      throw new TiBatchWriteException("Cannot extract handle non pk is handle table");
    }
  }

  private byte[] encodeTiRow(Row tiRow) {
    Object[] objects = new Object[columns.size()];
    for (int i = 0; i < objects.length; i++) {
      TiColumnInfo tiColumnInfo = columns.get(i);
      Object value = tiRow.get(i, tiColumnInfo.getType());
      objects[i] = value;
    }
    try {
      return TableCodec.encodeRow(columns, objects, tiTableInfo.isPkHandle(), enableNewRowFormat);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private Pair<byte[], Boolean> buildIndexKey(Row tiRow, Handle handle, TiIndexInfo index) {
    EncodeIndexDataResult encodeIndexDataResult =
        IndexKey.genIndexKey(tiTableInfo.getId(), tiRow, index, handle, tiTableInfo);
    return new Pair<>(encodeIndexDataResult.indexKey, encodeIndexDataResult.distinct);
  }

  private BytePairWrapper generateIndexKeyValue(
      Row tiRow, Handle handle, TiIndexInfo index, boolean remove) {
    Pair<byte[], Boolean> pair = buildIndexKey(tiRow, handle, index);

    byte[] key = pair.first;
    boolean distinct = pair.second;
    byte[] value;
    if (remove) {
      value = EMPTY_BYTES;
    } else {
      value = TableCodec.genIndexValue(handle, distinct);
    }
    return new BytePairWrapper(key, value);
  }

  private BytePairWrapper generateRecordKeyValue(Row tiRow, Handle handle, boolean remove) {
    byte[] key;
    byte[] value;
    if (remove) {
      key = RowKey.toRowKey(tiTableInfo.getId(), handle).getBytes();
      value = new byte[0];
    } else {
      key = RowKey.toRowKey(tiTableInfo.getId(), handle).getBytes();
      value = encodeTiRow(tiRow);
    }
    return new BytePairWrapper(key, value);
  }

  private List<BytePairWrapper> generateIndexKeyValues(Row tiRow, Handle handle, boolean remove) {
    return tiTableInfo.getIndices().stream()
        .filter(tiIndexInfo -> !(isCommonHandle && tiIndexInfo.isPrimary()))
        .map(tiIndexInfo -> generateIndexKeyValue(tiRow, handle, tiIndexInfo, remove))
        .collect(Collectors.toList());
  }

  /**
   * Generate the key-value pair conflicted with the given row. <br>
   *
   * <ul>
   *   <li>- unique index conflict
   *       <ul>
   *         <li>- key: encoded index key
   *         <li>- value: empty
   *       </ul>
   *   <li>- primary index conflict
   *       <ul>
   *         <li>- key: encoded row key
   *         <li>- value: empty
   *       </ul>
   * </ul>
   */
  private Map<ByteBuffer, BytePairWrapper> fetchConflictedRowDeletionPairs(
      Row tiRow, Handle handle) {
    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp.getPrevious());
    List<Pair<Row, Handle>> deletion = new ArrayList<>();
    if (handleCol != null || isCommonHandle) {
      getOldRowWithClusterIndex(handle, snapshot).ifPresent(deletion::add);
    }
    for (TiIndexInfo index : uniqueIndices) {
      // pk is uk when isCommonHandle, so we need to exclude it
      if (!isCommonHandle || !index.isPrimary()) {
        getOldRowWithUniqueIndexKey(index, handle, tiRow).ifPresent(deletion::add);
      }
    }
    Map<ByteBuffer, BytePairWrapper> deletionKeyValue = new HashMap<>();
    for (Pair<Row, Handle> pair : deletion) {
      BytePairWrapper recordKeyValue = generateRecordKeyValue(pair.first, pair.second, true);
      deletionKeyValue.put(ByteBuffer.wrap(recordKeyValue.getKey()), recordKeyValue);
      List<BytePairWrapper> indexKeyValues = generateIndexKeyValues(pair.first, pair.second, true);
      indexKeyValues.forEach(
          indexKeyValue ->
              deletionKeyValue.put(ByteBuffer.wrap(indexKeyValue.getKey()), indexKeyValue));
    }
    return deletionKeyValue;
  }

  public Optional<Pair<Row, Handle>> getOldRowWithUniqueIndexKey(
      TiIndexInfo index, Handle handle, Row tiRow) {
    Pair<byte[], Boolean> uniqueIndexKeyPair = buildIndexKey(tiRow, handle, index);
    if (uniqueIndexKeyPair.second) {
      byte[] oldValue = snapshot.get(uniqueIndexKeyPair.first);
      if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
        Handle oldHandle = TableCodec.decodeHandle(oldValue,!handle.isInt());
        byte[] oldRowValue =
            snapshot.get(RowKey.toRowKey(tiTableInfo.getId(), oldHandle).getBytes());
        Row oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo);
        return Optional.of(new Pair<>(oldRow, oldHandle));
      }
    }
    return Optional.empty();
  }

  public Optional<Pair<Row, Handle>> getOldRowWithClusterIndex(Handle handle, Snapshot snapshot) {
    byte[] key = RowKey.toRowKey(tiTableInfo.getId(), handle).getBytes();
    byte[] oldValue = snapshot.get(key);
    if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
      Row oldRow = TableCodec.decodeRow(oldValue, handle, tiTableInfo);
      return Optional.of(new Pair<>(oldRow, handle));
    }
    return Optional.empty();
  }

  public List<BytePairWrapper> generateKeyValuesByRow(Row row) {
    Preconditions.checkArgument(
        row.fieldCount() == columns.size(), "Columns and values do not match");
    List<BytePairWrapper> keyValues = new ArrayList<>();
    // set auto increment
    if (autoIncrementColumnIndex.isPresent()) {
      int index = autoIncrementColumnIndex.get();
      if (ignoreAutoincrementColumn) {
        row.set(index, columns.get(index).getType(), rowIdAllocator.getAutoIncId());
      } else if (row.isNull(index)) {
        throw new IllegalStateException("Auto increment column can not be null");
      }
    }

    if (isCommonHandle && !session.supportClusteredIndex()) {
      throw new TiBatchWriteException(
          "Current TiDB version does not support clustered index, please make sure TiDB version is greater than or equal to v5.0");
    }

    Handle handle;
    Map<ByteBuffer, BytePairWrapper> conflictRowDeletionPairs = new HashMap<>();
    boolean constraintCheckIsNeeded =
        isCommonHandle || handleCol != null || uniqueIndices.size() > 0;
    if (constraintCheckIsNeeded) {
      if (isCommonHandle || tiTableInfo.isPkHandle()) {
        handle = extractHandle(row);
      } else {
        handle = new IntHandle(rowIdAllocator.getSharedRowId());
      }
      // get deletion row
      conflictRowDeletionPairs = fetchConflictedRowDeletionPairs(row, handle);
      if (conflictRowDeletionPairs.size() > 0 && !replace) {
        throw new IllegalStateException(
            "Unique index conflicts, please use upsert mode, row = " + row);
      }
    } else {
      handle = new IntHandle(rowIdAllocator.getSharedRowId());
    }
    keyValues.add(generateRecordKeyValue(row, handle, false));
    keyValues.addAll(generateIndexKeyValues(row, handle, false));

    // if the key needs to be updated, then the deletion pair need to be removed and keep
    // the update pair. Otherwise, the execution order can't be guaranteed, which may cause
    // the deletion of the row when the update pair is executed first and deletion pair second.
    for (BytePairWrapper keyValue : keyValues) {
      conflictRowDeletionPairs.remove(ByteBuffer.wrap(keyValue.getKey()));
    }
    keyValues.addAll(conflictRowDeletionPairs.values());

    return keyValues;
  }

  /**
   * Generate record and index kay-value of the given row to delete. It will only delete one row
   * including its index in every call.
   *
   * <p>Only support table with at least one pk or uk (for multiple-column uk, every column should
   * not be null) or an exception will be thrown.
   *
   * @param row
   * @return
   */
  public List<BytePairWrapper> generateKeyValuesToDeleteByRow(Row row) {
    // get first pk or uk with value
    Optional<TiIndexInfo> UniqueIndexKeyWithValue =
        uniqueIndices.stream()
            .filter(
                indices -> {
                  for (TiIndexColumn col : indices.getIndexColumns()) {
                    if (row.isNull(col.getOffset())) {
                      return false;
                    }
                  }
                  return true;
                })
            .findFirst();

    // check constraint
    Preconditions.checkArgument(
        isCommonHandle || handleCol != null || UniqueIndexKeyWithValue.isPresent(),
        "Delete is only support with pk or uk, and their value should not be null");

    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp.getPrevious());
    List<Pair<Row, Handle>> deletion = new ArrayList<>();

    // get deletion row
    if (handleCol != null || isCommonHandle) {
      Handle handle = extractHandle(row);
      getOldRowWithClusterIndex(handle, snapshot).ifPresent(deletion::add);
    } else {
      // just make buildUniqueIndexKey method works
      Handle fakeHandle = new IntHandle(0L);
      getOldRowWithUniqueIndexKey(UniqueIndexKeyWithValue.get(), fakeHandle, row)
          .ifPresent(deletion::add);
    }

    // generate BytePairWrapper
    List<BytePairWrapper> deletionKeyValue = new ArrayList<>();
    for (Pair<Row, Handle> pair : deletion) {
      deletionKeyValue.add(generateRecordKeyValue(pair.first, pair.second, true));
      deletionKeyValue.addAll(generateIndexKeyValues(pair.first, pair.second, true));
    }
    return deletionKeyValue;
  }

  @Override
  public void close() {}

  public ClientSession getSession() {
    return session;
  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public TiTableInfo getTiTableInfo() {
    return tiTableInfo;
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }

  public List<TiColumnInfo> getColumns() {
    return columns;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TiIndexInfo> getUniqueIndices() {
    return uniqueIndices;
  }

  public TiColumnInfo getHandleCol() {
    return handleCol;
  }

  public boolean isCommonHandle() {
    return isCommonHandle;
  }

  public boolean isEnableNewRowFormat() {
    return enableNewRowFormat;
  }
}
