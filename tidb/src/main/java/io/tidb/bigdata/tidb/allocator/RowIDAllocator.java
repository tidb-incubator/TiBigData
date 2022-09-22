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

package io.tidb.bigdata.tidb.allocator;

import com.google.common.primitives.UnsignedLongs;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator.RowIDAllocatorType;
import io.tidb.bigdata.tidb.codec.Codec.IntegerCodec;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.codec.CodecDataOutput;
import io.tidb.bigdata.tidb.codec.KeyUtils;
import io.tidb.bigdata.tidb.codec.MetaCodec;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.TiSession;
import org.tikv.common.exception.AllocateRowIDOverflowException;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.TwoPhaseCommitter;

/**
 * RowIDAllocator read current start from TiKV and write back 'start+step' back to TiKV. It designs
 * to allocate all id for data to be written at once, hence it does not need run inside a txn.
 *
 * <p>(start, end] is allocated
 */
public final class RowIDAllocator implements Serializable {

  private final TiSession session;
  private final long maxShardRowIDBits;
  private final long dbId;
  private final long step;
  private final boolean isUnsigned;
  private long end;
  private RowIDAllocatorType type;

  private static final Logger LOG = LoggerFactory.getLogger(RowIDAllocator.class);

  private RowIDAllocator(
      long maxShardRowIDBits,
      long dbId,
      long step,
      TiSession session,
      boolean isUnsigned,
      RowIDAllocatorType type) {
    this.maxShardRowIDBits = maxShardRowIDBits;
    this.dbId = dbId;
    this.step = step;
    this.session = session;
    this.isUnsigned = isUnsigned;
    this.type = type;
  }

  public long getAutoIncId(long index) {
    return index + getStart();
  }

  /**
   * @param index should >= 1
   * @return
   */
  public long getShardRowId(long index) {
    return getShardRowId(maxShardRowIDBits, index, index + getStart(), isUnsigned);
  }

  public static long getShardRowId(long shardBits, long shardSeed, long rowID, boolean isUnsigned) {
    if (shardBits <= 0 || shardBits >= 16) {
      return rowID;
    }

    int signBitLength = isUnsigned ? 0 : 1;
    // assert rowID < Math.pow(2, 64 - shardBits)

    long partition = shardSeed & ((1L << shardBits) - 1);
    return rowID | (partition << (64 - shardBits - signBitLength));
  }

  public static RowIDAllocator create(
      long dbId,
      TiTableInfo table,
      TiSession session,
      boolean isUnsigned,
      long step,
      long shardBits,
      RowIDAllocatorType type) {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(40000);
    while (true) {
      try {
        return doCreate(dbId, table, session, isUnsigned, step, shardBits, type);
      } catch (AllocateRowIDOverflowException | IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoServerBusy, e);
      }
    }
  }

  private static RowIDAllocator doCreate(
      long dbId,
      TiTableInfo table,
      TiSession session,
      boolean unsigned,
      long step,
      long shardBits,
      RowIDAllocatorType type) {
    RowIDAllocator allocator =
        new RowIDAllocator(table.getMaxShardRowIDBits(), dbId, step, session, unsigned, type);
    if (unsigned) {
      allocator.initUnsigned(session.createSnapshot(), table.getId(), shardBits);
    } else {
      allocator.initSigned(session.createSnapshot(), table.getId(), shardBits);
    }

    return allocator;
  }

  public long getStart() {
    return end - step;
  }

  public long getEnd() {
    return end;
  }

  // set key value pairs to tikv via two phase committer protocol.
  private void set(@Nonnull List<BytePairWrapper> pairs, @Nonnull TiTimestamp timestamp) {
    Iterator<BytePairWrapper> iterator = pairs.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(session, timestamp.getVersion(), 1000L);
    try {
      BytePairWrapper primaryPair = pairs.get(0);
      twoPhaseCommitter.prewritePrimaryKey(
          ConcreteBackOffer.newCustomBackOff(1000), primaryPair.getKey(), primaryPair.getValue());

      if (!iterator.hasNext()) {
        twoPhaseCommitter.prewriteSecondaryKeys(primaryPair.getKey(), iterator, 1000);
      }

      twoPhaseCommitter.commitPrimaryKey(
          ConcreteBackOffer.newCustomBackOff(1000),
          primaryPair.getKey(),
          session.getTimestamp().getVersion());
    } finally {
      try {
        twoPhaseCommitter.close();
      } catch (Throwable t) {
        LOG.warn("Closing twoPhaseCommitter failed.", t);
      }
    }
  }

  private Optional<BytePairWrapper> getMetaToUpdate(
      ByteString key, byte[] oldVal, Snapshot snapshot) {
    // 1. encode hash meta key
    // 2. load meta via hash meta key from TiKV
    // 3. update meta's filed count and set it back to TiKV
    CodecDataOutput cdo = new CodecDataOutput();
    ByteString metaKey = MetaCodec.encodeHashMetaKey(cdo, key.toByteArray());
    long fieldCount = 0;
    ByteString metaVal = snapshot.get(metaKey);

    // decode long from bytes
    // big endian the 8 bytes
    if (!metaVal.isEmpty()) {
      try {
        fieldCount = IntegerCodec.readULong(new CodecDataInput(metaVal.toByteArray()));
      } catch (Exception ignored) {
        LOG.warn("metaDecode failed, field is ignored." + KeyUtils.formatBytesUTF8(metaVal));
      }
    }

    // update meta field count only oldVal is null
    if (oldVal == null || oldVal.length == 0) {
      fieldCount++;
      cdo.reset();
      cdo.writeLong(fieldCount);

      return Optional.of(new BytePairWrapper(metaKey.toByteArray(), cdo.toBytes()));
    }
    return Optional.empty();
  }

  private long updateHash(
      ByteString key,
      ByteString field,
      Function<byte[], byte[]> calculateNewVal,
      Snapshot snapshot) {
    // 1. encode hash data key
    // 2. get value in byte from get operation
    // 3. calculate new value via calculateNewVal
    // 4. check old value equals to new value or not
    // 5. set the new value back to TiKV via 2pc
    // 6. encode a hash meta key
    // 7. update a hash meta field count if needed

    CodecDataOutput cdo = new CodecDataOutput();
    MetaCodec.encodeHashDataKey(cdo, key.toByteArray(), field.toByteArray());
    ByteString dataKey = cdo.toByteString();
    byte[] oldVal = snapshot.get(dataKey.toByteArray());

    byte[] newVal = calculateNewVal.apply(oldVal);
    if (Arrays.equals(newVal, oldVal)) {
      // not need to update
      return 0L;
    }

    ArrayList<BytePairWrapper> pairs = new ArrayList<>(2);
    pairs.add(new BytePairWrapper(dataKey.toByteArray(), newVal));
    getMetaToUpdate(key, oldVal, snapshot).ifPresent(pairs::add);
    set(pairs, snapshot.getTimestamp());
    return Long.parseLong(new String(newVal));
  }

  private static boolean isDBExisted(long dbId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString json = MetaCodec.hashGet(MetaCodec.KEY_DBs, dbKey, snapshot);
    return json != null && !json.isEmpty();
  }

  private static boolean isTableExisted(long dbId, long tableId, Snapshot snapshot) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    ByteString tableKey = MetaCodec.tableKey(tableId);
    return !MetaCodec.hashGet(dbKey, tableKey, snapshot).isEmpty();
  }

  public static boolean shardRowBitsOverflow(
      long base, long step, long shardRowBits, boolean reservedSignBit) {
    long signBit = reservedSignBit ? 1 : 0;
    long mask = ((1L << shardRowBits) - 1) << (64 - shardRowBits - signBit);
    if (reservedSignBit) {
      return ((base + step) & mask) > 0;
    } else {
      return Long.compareUnsigned((base + step) & mask, 0) > 0;
    }
  }

  /**
   * read current row id from TiKV and write the calculated value back to TiKV. The calculation rule
   * is start(read from TiKV) + step.
   */
  public long updateAllocateId(
      long dbId, long tableId, long step, Snapshot snapshot, long shard, boolean hasSignedBit) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      return updateHash(
          MetaCodec.encodeDatabaseID(dbId),
          getIdField(tableId, type),
          (oldVal) -> {
            long base = 0;
            if (oldVal != null && oldVal.length != 0) {
              base = Long.parseLong(new String(oldVal));
            }
            if (shard >= 1 && shardRowBitsOverflow(base, step, shard, hasSignedBit)) {
              throw new AllocateRowIDOverflowException(base, step, shard);
            }
            base += step;
            return String.valueOf(base).getBytes();
          },
          snapshot);
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  /** read current row id from TiKV according to database id and table id. */
  public static long getAllocateId(
      long dbId, long tableId, Snapshot snapshot, RowIDAllocatorType allocatorType) {
    if (isDBExisted(dbId, snapshot) && isTableExisted(dbId, tableId, snapshot)) {
      ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
      ByteString tblKey = getIdField(tableId, allocatorType);
      ByteString val = MetaCodec.hashGet(dbKey, tblKey, snapshot);
      if (val.isEmpty()) {
        return 0L;
      }
      return Long.parseLong(val.toStringUtf8());
    }

    throw new IllegalArgumentException("table or database is not existed");
  }

  public static ByteString getIdField(long tableId, RowIDAllocatorType allocatorType) {
    switch (allocatorType) {
      case AUTO_INCREMENT:
      case IMPLICIT_ROWID:
        return MetaCodec.autoTableIDKey(tableId);
      case AUTO_RANDOM:
        return MetaCodec.autoRandomTableIDKey(tableId);
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + allocatorType);
    }
  }

  private void initSigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot, type);
    long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    if (newStart == Long.MAX_VALUE) {
      throw new TiBatchWriteException("cannot allocate more ids since it ");
    }
    end = updateAllocateId(dbId, tableId, tmpStep, snapshot, shard, true);
  }

  private void initUnsigned(Snapshot snapshot, long tableId, long shard) {
    // get new start from TiKV, and calculate new end and set it back to TiKV.
    long newStart = getAllocateId(dbId, tableId, snapshot, type);
    // for unsigned long, -1L is max value.
    long tmpStep = UnsignedLongs.min(-1L - newStart, step);
    if (tmpStep != step) {
      throw new TiBatchWriteException("cannot allocate ids for this write");
    }
    // when compare unsigned long, the min value is largest value.
    if (UnsignedLongs.compare(newStart, -1L) == 0) {
      throw new TiBatchWriteException(
          "cannot allocate more ids since the start reaches " + "unsigned long's max value ");
    }
    end = updateAllocateId(dbId, tableId, tmpStep, snapshot, shard, false);
  }
}
