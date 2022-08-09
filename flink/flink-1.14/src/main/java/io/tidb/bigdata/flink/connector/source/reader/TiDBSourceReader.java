/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.source.reader;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SOURCE_FAILOVER;

import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;

public class TiDBSourceReader implements SourceReader<RowData, TiDBSourceSplit> {

  private final Queue<TiDBSourceSplit> remainingSplits;
  private final SourceReaderContext context;
  private final Map<String, String> properties;
  private final List<ColumnHandleInternal> columns;
  private final TiDBSchemaAdapter schema;
  private final Expression expression;
  private final Integer limit;
  private final FailoverType failoverType;

  private ClientSession session;

  /** The availability future. This reader is available as soon as a split is assigned. */
  private CompletableFuture<Void> availability;

  private TiDBSourceSplit currentSplit;
  private long offset;
  private RecordCursorInternal cursor;

  private boolean noMoreSplits;

  public TiDBSourceReader(
      SourceReaderContext context,
      Map<String, String> properties,
      List<ColumnHandleInternal> columns,
      TiDBSchemaAdapter schema,
      Expression expression,
      Integer limit) {
    this.context = context;
    this.properties = properties;
    this.columns = columns;
    this.schema = schema;
    this.expression = expression;
    this.limit = limit;
    this.availability = new CompletableFuture<>();
    this.remainingSplits = new ArrayDeque<>();
    this.failoverType =
        FailoverType.fromString(
            properties.getOrDefault(SOURCE_FAILOVER.key(), SOURCE_FAILOVER.defaultValue()));
  }

  @Override
  public void start() {
    // request a split if we don't have one
    if (remainingSplits.isEmpty()) {
      context.sendSplitRequest();
    }
    session = ClientSession.create(new ClientConfig(properties));
  }

  private void finishSplit() {
    currentSplit = null;
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    // request another split if no other is left
    // we do this only here in the finishSplit part to avoid requesting a split
    // whenever the reader is polled and doesn't currently have a split
    if (remainingSplits.isEmpty() && !noMoreSplits) {
      context.sendSplitRequest();
    }
  }

  private InputStatus tryMoveToNextSplit() {
    currentSplit = remainingSplits.poll();
    if (currentSplit != null) {
      SplitInternal split = currentSplit.getSplit();
      offset = currentSplit.getOffset();
      cursor =
          new RecordSetInternal(
                  session,
                  split,
                  columns,
                  Optional.ofNullable(expression),
                  Optional.ofNullable(split.getTimestamp()),
                  Optional.ofNullable(limit))
              .cursor();
      // skip offset
      for (int i = 0; i < offset; i++) {
        if (!cursor.advanceNextPosition()) {
          break;
        }
      }
      return InputStatus.MORE_AVAILABLE;
    } else if (noMoreSplits) {
      return InputStatus.END_OF_INPUT;
    } else {
      // ensure we are not called in a loop by resetting the availability future
      if (availability.isDone()) {
        availability = new CompletableFuture<>();
      }
      return InputStatus.NOTHING_AVAILABLE;
    }
  }

  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    if (cursor != null && cursor.advanceNextPosition()) {
      offset++;
      output.collect(schema.convert(currentSplit.getSplit().getTimestamp(), cursor));
      return InputStatus.MORE_AVAILABLE;
    } else {
      finishSplit();
    }
    return tryMoveToNextSplit();
  }

  @Override
  public List<TiDBSourceSplit> snapshotState(long checkpointId) {
    if (currentSplit == null && remainingSplits.isEmpty()) {
      return Collections.emptyList();
    }
    final ArrayList<TiDBSourceSplit> splits = new ArrayList<>(1 + remainingSplits.size());
    if (currentSplit != null) {
      // Add back to snapshot
      if (failoverType == FailoverType.SPLIT) {
        splits.add(currentSplit);
      } else {
        splits.add(new TiDBSourceSplit(currentSplit.getSplit(), offset));
      }
    }
    splits.addAll(remainingSplits);
    return splits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return availability;
  }

  @Override
  public void addSplits(List<TiDBSourceSplit> splits) {
    remainingSplits.addAll(splits);
    // set availability so that pollNext is actually called
    availability.complete(null);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.noMoreSplits = true;
    // set availability so that pollNext is actually called
    availability.complete(null);
  }

  @Override
  public void close() throws Exception {
    if (cursor != null) {
      cursor.close();
    }
    if (session != null) {
      session.close();
    }
  }

  public enum FailoverType {
    SPLIT,
    OFFSET;

    public static FailoverType fromString(String s) {
      return Arrays.stream(values())
          .filter(value -> value.name().equalsIgnoreCase(s))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported failover type: " + s));
    }
  }
}
