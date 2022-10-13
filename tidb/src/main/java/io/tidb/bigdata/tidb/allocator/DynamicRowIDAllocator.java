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

import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.codec.digest.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public class DynamicRowIDAllocator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicRowIDAllocator.class);
  private static final ThreadLocal<Random> RANDOM_THREAD_LOCAL =
      ThreadLocal.withInitial(Random::new);

  private final ClientSession session;
  private final String databaseName;
  private final String tableName;
  private final int step;
  private final long shardBits;
  private final TiTimestamp startTimestamp;
  private final RowIDAllocatorType type;
  private final boolean isUnsigned;
  private final TiTableInfo tableInfo;
  private Long start;
  private ThreadPoolExecutor threadPool;
  private FutureTask<Long> futureTask;
  private int index;
  private long currentShardSeed;

  public DynamicRowIDAllocator(
      ClientSession session,
      String databaseName,
      String tableName,
      int step,
      TiTimestamp startTimestamp) {
    this(session, databaseName, tableName, step, null, startTimestamp);
  }

  public DynamicRowIDAllocator(
      ClientSession session,
      String databaseName,
      String tableName,
      int step,
      @Nullable Long start,
      TiTimestamp startTimestamp) {
    this.session = session;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.step = step;
    this.start = start;
    this.startTimestamp = startTimestamp;
    this.type = getType();
    this.shardBits = getShardBits();
    this.isUnsigned = isUnsigned();
    this.tableInfo = session.getTableMust(databaseName, tableName);
    initRandomGenerator();
  }

  private void initRandomGenerator() {
    long version = startTimestamp.getVersion();
    Random random = RANDOM_THREAD_LOCAL.get();
    random.setSeed(version);
  }

  private boolean isUnsigned() {
    TiTableInfo tableInfo = session.getTableMust(databaseName, tableName);
    switch (type) {
      case AUTO_INCREMENT:
        return tableInfo.isAutoIncrementColUnsigned();
      case AUTO_RANDOM:
        return tableInfo.isAutoRandomColUnsigned();
      case IMPLICIT_ROWID:
        // IMPLICIT_ROWID is always signed.
        return false;
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + type);
    }
  }

  private RowIDAllocatorType getType() {
    TiTableInfo tableInfo = session.getTableMust(databaseName, tableName);
    if (tableInfo.hasAutoRandomColumn()) {
      return RowIDAllocatorType.AUTO_RANDOM;
    } else if (tableInfo.hasAutoIncrementColumn()) {
      return RowIDAllocatorType.AUTO_INCREMENT;
    } else {
      return RowIDAllocatorType.IMPLICIT_ROWID;
    }
  }

  private long getShardBits() {
    TiTableInfo tableInfo = session.getTableMust(databaseName, tableName);
    switch (type) {
      case AUTO_INCREMENT:
        // AUTO_INC doesn't have shard bits.
        return 0L;
      case AUTO_RANDOM:
        return tableInfo.getAutoRandomBits();
      case IMPLICIT_ROWID:
        return tableInfo.getMaxShardRowIDBits();
      default:
        throw new IllegalArgumentException("Unsupported RowIDAllocatorType: " + type);
    }
  }

  private void checkUpdate() {
    if (start == null) {
      start = session.createRowIdAllocator(databaseName, tableInfo, step, type).getStart();
      updateCurrentShard();
    }
    if (index == (int) (step * 0.8)) {
      if (threadPool == null) {
        this.threadPool =
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));
      }
      // async get next row id allocator
      LOG.info("Get next row id range asynchronously...");

      /**
       * TODO: enhancement. Every workNode in Flink has its own row id allocator and every time
       * allocator will get current row id range, so each allocator will query for the same range
       * which cause a lot of data race.
       */
      futureTask =
          new FutureTask<>(
              () -> session.createRowIdAllocator(databaseName, tableInfo, step, type).getStart());
      threadPool.submit(futureTask);
    }
    if (index >= step) {
      try {
        start = futureTask.get();
        updateCurrentShard();
        futureTask = null;
        index = 0;
      } catch (Exception e) {
        throw new IllegalStateException("Can not get next row id range", e);
      }
    }
  }

  private void updateCurrentShard() {
    if (type != RowIDAllocatorType.AUTO_RANDOM) {
      return;
    }

    Random random = RANDOM_THREAD_LOCAL.get();
    // Use MurmurHash3 to make sure the bits of shardSeed >= 15
    currentShardSeed = MurmurHash3.hash32(random.nextLong());
  }

  public long getImplicitRowId() {
    checkUpdate();
    index++;
    return RowIDAllocator.getShardRowId(shardBits, index, index + start, isUnsigned);
  }

  public long getAutoRandomId() {
    checkUpdate();
    index++;
    return RowIDAllocator.getShardRowId(shardBits, currentShardSeed, index + start, isUnsigned);
  }

  public long getAutoIncrementId() {
    checkUpdate();
    index++;
    return index + start;
  }

  @Override
  public void close() {
    Optional.ofNullable(threadPool).ifPresent(ThreadPoolExecutor::shutdownNow);
  }

  public enum RowIDAllocatorType {
    AUTO_INCREMENT,
    AUTO_RANDOM,
    IMPLICIT_ROWID
  }
}
