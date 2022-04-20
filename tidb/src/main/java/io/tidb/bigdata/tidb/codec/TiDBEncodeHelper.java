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
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.IntHandle;
import io.tidb.bigdata.tidb.key.IndexKey;
import io.tidb.bigdata.tidb.key.IndexKey.EncodeIndexDataResult;
import io.tidb.bigdata.tidb.key.Key;
import io.tidb.bigdata.tidb.key.RowKey;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  public static final byte[] ZERO_BYTES = new byte[]{'0'};

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

  public TiDBEncodeHelper(ClientSession session, TiTimestamp timestamp, String databaseName,
      String tableName, boolean ignoreAutoincrementColumn,
      boolean replace, DynamicRowIDAllocator rowIDAllocator) {
    this.session = session;
    this.timestamp = timestamp;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.ignoreAutoincrementColumn = ignoreAutoincrementColumn;
    this.tiTableInfo = session.getTableMust(databaseName, tableName);
    this.snapshot = session.getTiSession().createSnapshot(timestamp);
    this.columns = tiTableInfo.getColumns();
    this.columnNames = columns.stream().map(TiColumnInfo::getName).collect(Collectors.toList());
    this.autoIncrementColumnIndex = Optional.ofNullable(tiTableInfo.getAutoIncrementColInfo()).map(
        TiColumnInfo::getOffset);
    this.uniqueIndices = tiTableInfo.getIndices().stream()
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
      return new IntHandle((long) tiRow.get(handleCol.getOffset(), handleCol.getType()));
    } else if (isCommonHandle) {
      throw new TiBatchWriteException("Cannot extract handle non pk is handle table");
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
      return TableCodec.encodeRow(
          columns,
          objects,
          tiTableInfo.isPkHandle(),
          enableNewRowFormat);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private Pair<byte[], Boolean> buildUniqueIndexKey(Row tiRow, Handle handle, TiIndexInfo index) {
    EncodeIndexDataResult encodeIndexDataResult = IndexKey.encodeIndexDataValues(
        tiRow,
        index.getIndexColumns(),
        handle,
        index.isUnique() && !index.isPrimary(),
        tiTableInfo);
    Key[] keys = encodeIndexDataResult.keys;
    IndexKey indexKey = IndexKey.toIndexKey(tiTableInfo.getId(), index.getId(), keys);
    return new Pair<>(indexKey.getBytes(), encodeIndexDataResult.appendHandle);
  }

  private BytePairWrapper generateUniqueIndexKeyValue(Row tiRow, Handle handle,
      TiIndexInfo index, boolean remove) {
    Pair<byte[], Boolean> pair = buildUniqueIndexKey(tiRow, handle, index);
    byte[] key = pair.first;
    byte[] value;
    if (remove) {
      value = EMPTY_BYTES;
    } else {
      if (pair.second) {
        value = ZERO_BYTES;
      } else {
        // TODO clustered index
        CodecDataOutput codecDataOutput = new CodecDataOutput();
        codecDataOutput.writeLong(handle.intValue());
        value = codecDataOutput.toBytes();
      }
    }
    return new BytePairWrapper(key, value);
  }

  private BytePairWrapper generateSecondaryIndexKeyValue(Row tiRow, Handle handle,
      TiIndexInfo index, boolean remove) {
    Key[] keys = IndexKey.encodeIndexDataValues(tiRow, index.getIndexColumns(), handle, false,
        tiTableInfo).keys;
    CodecDataOutput codecDataOutput = new CodecDataOutput();
    codecDataOutput.write(IndexKey.toIndexKey(tiTableInfo.getId(), index.getId(), keys).getBytes());
    codecDataOutput.write(handle.encodedAsKey());
    byte[] value;
    if (remove) {
      value = EMPTY_BYTES;
    } else {
      value = ZERO_BYTES;
    }
    return new BytePairWrapper(codecDataOutput.toBytes(), value);
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

  private List<BytePairWrapper> generateIndexKeyValues(Row tiRow, Handle handle,
      boolean remove) {
    return tiTableInfo.getIndices().stream()
        .filter(tiIndexInfo -> !(isCommonHandle && tiIndexInfo.isPrimary()))
        .map(tiIndexInfo -> {
          if (tiIndexInfo.isUnique()) {
            return generateUniqueIndexKeyValue(tiRow, handle, tiIndexInfo, remove);
          } else {
            return generateSecondaryIndexKeyValue(tiRow, handle, tiIndexInfo, remove);
          }
        }).collect(Collectors.toList());
  }

  private List<BytePairWrapper> generateDataToBeRemoved(Row tiRow, Handle handle) {
    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp.getPrevious());
    List<Pair<Row, Handle>> deletion = new ArrayList<>();
    if (handleCol != null || isCommonHandle) {
      byte[] key = RowKey.toRowKey(tiTableInfo.getId(), handle).getBytes();
      byte[] oldValue = snapshot.get(key);
      if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
        Row oldRow = TableCodec.decodeRow(oldValue, handle, tiTableInfo);
        deletion.add(new Pair<>(oldRow, handle));
      }
    }
    for (TiIndexInfo index : uniqueIndices) {
      if (!isCommonHandle || !index.isPrimary()) {
        Pair<byte[], Boolean> uniqueIndexKeyPair = buildUniqueIndexKey(tiRow, handle, index);
        if (!uniqueIndexKeyPair.second) {
          byte[] oldValue = snapshot.get(uniqueIndexKeyPair.first);
          if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
            Handle oldHandle = TableCodec.decodeHandle(oldValue, false);
            byte[] oldRowValue = snapshot.get(
                RowKey.toRowKey(tiTableInfo.getId(), oldHandle).getBytes());
            Row oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo);
            deletion.add(new Pair<>(oldRow, oldHandle));
          }
        }
      }
    }
    List<BytePairWrapper> deletionKeyValue = new ArrayList<>();
    for (Pair<Row, Handle> pair : deletion) {
      deletionKeyValue.add(generateRecordKeyValue(pair.first, pair.second, true));
      deletionKeyValue.addAll(generateIndexKeyValues(pair.first, pair.second, true));
    }
    return deletionKeyValue;
  }

  public List<BytePairWrapper> generateKeyValuesByRow(Row row) {
    Preconditions.checkArgument(row.fieldCount() == columns.size(),
        "Columns and values do not match");
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
    // TODO cluster index
    if (isCommonHandle) {
      throw new TiBatchWriteException("Clustered index does not supported now");
    }
    Handle handle;
    boolean constraintCheckIsNeeded = isCommonHandle
        || handleCol != null
        || uniqueIndices.size() > 0;
    if (constraintCheckIsNeeded) {
      if (isCommonHandle || tiTableInfo.isPkHandle()) {
        handle = extractHandle(row);
      } else {
        handle = new IntHandle(rowIdAllocator.getSharedRowId());
      }
      // get deletion row
      List<BytePairWrapper> dataToBeRemoved = generateDataToBeRemoved(row, handle);
      if (dataToBeRemoved.size() > 0 && !replace) {
        throw new IllegalStateException(
            "Unique index conflicts, please use upsert mode, row = " + row);
      }
      keyValues.addAll(dataToBeRemoved);
    } else {
      handle = new IntHandle(rowIdAllocator.getSharedRowId());
    }
    keyValues.add(generateRecordKeyValue(row, handle, false));
    keyValues.addAll(generateIndexKeyValues(row, handle, false));
    return keyValues;
  }

  @Override
  public void close() {
  }

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