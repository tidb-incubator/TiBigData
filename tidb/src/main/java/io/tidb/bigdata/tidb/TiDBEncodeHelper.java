package io.tidb.bigdata.tidb;

import com.google.common.base.Preconditions;
import com.pingcap.tikv.key.IntHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.StoreVersion;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.key.IndexKey;
import org.tikv.common.key.IndexKey.EncodeIndexDataResult;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.Row;
import org.tikv.common.util.Pair;

public class TiDBEncodeHelper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBEncodeHelper.class);

  public static final String VERSION = "4.0.0";
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] ZERO_BYTES = new byte[]{'0'};
  public static final int STEP_DEFAULT = 30000;


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

  public TiDBEncodeHelper(ClientSession session, TiTimestamp timestamp, String databaseName,
      String tableName) {
    this(session, null, timestamp, databaseName, tableName, true);
  }

  public TiDBEncodeHelper(
      ClientSession session,
      @Nullable DynamicRowIDAllocator rowIdAllocator,
      TiTimestamp timestamp,
      String databaseName,
      String tableName,
      boolean ignoreAutoincrementColumn) {
    this.session = session;
    this.rowIdAllocator = Optional.ofNullable(rowIdAllocator)
        .orElse(new DynamicRowIDAllocator(session, databaseName, tableName, STEP_DEFAULT));
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
  }

  private boolean isNullUniqueIndexValue(byte[] value) {
    return Arrays.equals(value, ZERO_BYTES);
  }

  private boolean isEmptyArray(byte[] array) {
    return array == null || array.length == 0;
  }

  private long extractHandle(Row tiRow) {
    if (tiTableInfo.isPkHandle()) {
      return (long) tiRow.get(handleCol.getOffset(), handleCol.getType());
    } else if (isCommonHandle) {
      throw new TiBatchWriteException("Clustered index does not supported now");
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

  private Pair<byte[], Boolean> buildUniqueIndexKey(Row tiRow, long handle, TiIndexInfo index) {
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

  private BytePairWrapper generateUniqueIndexKeyValue(Row tiRow, long handle,
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
        codecDataOutput.writeLong(handle);
        value = codecDataOutput.toBytes();
      }
    }
    return new BytePairWrapper(key, value);
  }

  private BytePairWrapper generateSecondaryIndexKeyValue(Row tiRow, long handle,
      TiIndexInfo index, boolean remove) {
    Key[] keys = IndexKey.encodeIndexDataValues(tiRow, index.getIndexColumns(), handle, false,
        tiTableInfo).keys;
    CodecDataOutput codecDataOutput = new CodecDataOutput();
    codecDataOutput.write(IndexKey.toIndexKey(tiTableInfo.getId(), index.getId(), keys).getBytes());
    codecDataOutput.write(new IntHandle(handle).encodedAsKey());
    byte[] value;
    if (remove) {
      value = EMPTY_BYTES;
    } else {
      value = ZERO_BYTES;
    }
    return new BytePairWrapper(codecDataOutput.toBytes(), value);
  }

  private BytePairWrapper generateRecordKeyValue(Row tiRow, long handle, boolean remove) {
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

  private List<BytePairWrapper> generateIndexKeyValues(Row tiRow, long handle,
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

  private List<BytePairWrapper> generateDataToBeRemoved(Row tiRow, long handle) {
    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp.getPrevious());
    List<Pair<Row, Long>> deletion = new ArrayList<>();
    // 主键
    if (handleCol != null || isCommonHandle) {
      byte[] key = RowKey.toRowKey(tiTableInfo.getId(), handle).getBytes();
      byte[] oldValue = snapshot.get(key);
      if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
        Row oldRow = TableCodec.decodeRow(oldValue, handle, tiTableInfo);
        deletion.add(new Pair<>(oldRow, handle));
      }
    }
    // 唯一索引
    for (TiIndexInfo index : uniqueIndices) {
      if (!isCommonHandle || !index.isPrimary()) {
        Pair<byte[], Boolean> uniqueIndexKeyPair = buildUniqueIndexKey(tiRow, handle, index);
        if (!uniqueIndexKeyPair.second) {
          byte[] oldValue = snapshot.get(uniqueIndexKeyPair.first);
          if (!isEmptyArray(oldValue) && !isNullUniqueIndexValue(oldValue)) {
            long oldHandle = TableCodec.decodeHandle(oldValue);
            byte[] oldRowValue = snapshot.get(
                RowKey.toRowKey(tiTableInfo.getId(), oldHandle).getBytes());
            Row oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo);
            deletion.add(new Pair<>(oldRow, oldHandle));
          }
        }
      }
    }
    List<BytePairWrapper> deletionKeyValue = new ArrayList<>();
    for (Pair<Row, Long> pair : deletion) {
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
    // 检查主键
    // TODO cluster index
    if (isCommonHandle) {
      throw new TiBatchWriteException("Clustered index does not supported now");
    }
    long handle;
    boolean constraintCheckIsNeeded = isCommonHandle
        || handleCol != null
        || uniqueIndices.size() > 0;
    if (constraintCheckIsNeeded) {
      if (isCommonHandle || tiTableInfo.isPkHandle()) {
        handle = extractHandle(row);
      } else {
        handle = rowIdAllocator.getSharedRowId();
      }
      // get deletion row
      keyValues.addAll(generateDataToBeRemoved(row, handle));
    } else {
      handle = rowIdAllocator.getSharedRowId();
    }
    keyValues.add(generateRecordKeyValue(row, handle, false));
    keyValues.addAll(generateIndexKeyValues(row, handle, false));
    return keyValues;
  }

  @Override
  public void close() {
    rowIdAllocator.close();
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
