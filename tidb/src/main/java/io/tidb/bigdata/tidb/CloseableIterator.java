package io.tidb.bigdata.tidb;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.tidb.key.Base64KeyRange;
import io.tidb.bigdata.tidb.meta.TiDAGRequest;
import io.tidb.bigdata.tidb.meta.TiDAGRequest.Builder;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.Row;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

  @Override
  void close();

  static <T> CloseableIterator<T> create(Iterator<T> iterator) {
    return new IteratorWrap<>(iterator);
  }

  static CloseableIterator<Row> create(
      ClientConfig config, Builder request, List<Base64KeyRange> ranges) {
    return new MemoryCoprocessorIterator(config, request, ranges);
  }

  class IteratorWrap<T> implements CloseableIterator<T> {

    private final Iterator<T> iterator;

    public IteratorWrap(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }
  }

  /**
   * Fetch all rows in memory for each region.
   */
  class MemoryCoprocessorIterator implements CloseableIterator<Row> {

    static final Logger LOG = LoggerFactory.getLogger(MemoryCoprocessorIterator.class);

    private final TiDAGRequest.Builder request;
    private final LinkedList<Base64KeyRange> ranges;
    private final List<ClientSession> sessions;
    private Iterator<Row> iterator;
    private boolean closed;

    public MemoryCoprocessorIterator(
        ClientConfig config, Builder request, List<Base64KeyRange> ranges) {
      this.request = request;
      this.ranges = new LinkedList<>(ranges);
      this.sessions = createSessionForRoles(config);
      nextIterator();
    }

    private List<ClientSession> createSessionForRoles(ClientConfig config) {
      ReplicaReadPolicy replicaReadPolicy = config.getReplicaReadPolicy();
      return replicaReadPolicy.getRoles().stream()
          .map(
              role -> {
                ReplicaReadPolicy policy =
                    new ReplicaReadPolicy(
                        replicaReadPolicy.getLabels(),
                        replicaReadPolicy.getWhitelist(),
                        replicaReadPolicy.getBlacklist(),
                        ImmutableList.of(role));
                ClientConfig clientConfig = new ClientConfig(config);
                clientConfig.setReplicaReadPolicy(policy);
                return ClientSession.create(clientConfig);
              })
          .collect(Collectors.toList());
    }

    /**
     *
     * It is able to retry for all roles, because we stored all rows for each region.
     */
    private List<Row> fetchAllRows(Base64KeyRange range) {
      Exception lastException = null;
      for (ClientSession session : sessions) {
        try {
          List<Row> rows = new ArrayList<>();
          CoprocessorIterator<Row> iterate = session.iterate(request, range);
          while (iterate.hasNext()) {
            rows.add(iterate.next());
          }
          return rows;
        } catch (Exception e) {
          lastException = e;
          LOG.warn("Fetch rows for range: {} failed", range.toString());
        }
      }
      close();
      throw new IllegalStateException("Can not fetch rows for range: " + range, lastException);
    }

    private List<Row> tryIterateNextRange() {
      ClientSession session = sessions.get(0);
      Base64KeyRange range = ranges.removeFirst();
      // Ensure that each range corresponds to a region.
      List<Base64KeyRange> newRanges = session.reSplit(range);
      if (newRanges.size() == 0) {
        return ImmutableList.of();
      }
      ranges.addAll(0, newRanges);
      range = ranges.removeFirst();
      return fetchAllRows(range);
    }

    private boolean nextIterator() {
      while (ranges.size() > 0) {
        List<Row> rows = tryIterateNextRange();
        if (rows.size() == 0) {
          continue;
        }
        this.iterator = rows.iterator();
        return true;
      }
      return false;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = iterator.hasNext() || nextIterator();
      if (!hasNext) {
        // Make sure it is closed.
        close();
      }
      return hasNext;
    }

    @Override
    public Row next() {
      if (hasNext()) {
        return iterator.next();
      }
      throw new IllegalStateException("There are no more rows.");
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        sessions.forEach(
            session -> {
              try {
                session.close();
              } catch (Exception e) {
                // ignore
                LOG.warn("Closing session failed.", e);
              }
            });
      }
    }
  }
}
