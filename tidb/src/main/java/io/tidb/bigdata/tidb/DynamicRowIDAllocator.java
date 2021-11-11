package io.tidb.bigdata.tidb;

import com.pingcap.tikv.allocator.RowIDAllocator;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicRowIDAllocator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicRowIDAllocator.class);

  private final ClientSession session;
  private final String databaseName;
  private final String tableName;
  private final int step;
  private final long maxShardRowIDBits;

  private Long start;
  private ThreadPoolExecutor threadPool;
  private FutureTask<Long> futureTask;
  private int index;


  public DynamicRowIDAllocator(ClientSession session, String databaseName, String tableName,
      int step) {
    this(session, databaseName, tableName, step, null);
  }

  public DynamicRowIDAllocator(ClientSession session, String databaseName, String tableName,
      int step, @Nullable Long start) {
    this.session = session;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.step = step;
    this.start = start;
    this.maxShardRowIDBits = session.getTableMust(databaseName, tableName).getMaxShardRowIDBits();
  }

  private void checkUpdate() {
    if (start == null) {
      start = session.createRowIdAllocator(databaseName, tableName, step).getStart();
    }
    if (index == (int) (step * 0.8)) {
      if (threadPool == null) {
        this.threadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1));
      }
      // async get next row id allocator
      LOG.info("Get next row id range asynchronously...");
      futureTask = new FutureTask<>(
          () -> session.createRowIdAllocator(databaseName, tableName, step, 3).getStart());
      threadPool.submit(futureTask);
    }
    if (index >= step) {
      try {
        start = futureTask.get();
        futureTask = null;
        index = 0;
      } catch (Exception e) {
        throw new IllegalStateException("Can not get next row id range", e);
      }
    }
  }

  public long getSharedRowId() {
    checkUpdate();
    index++;
    return RowIDAllocator.getShardRowId(maxShardRowIDBits, index, index + start);
  }

  public long getAutoIncId() {
    checkUpdate();
    index++;
    return index + start;
  }

  @Override
  public void close() {
    Optional.ofNullable(threadPool).ifPresent(ThreadPoolExecutor::shutdownNow);
  }


}
