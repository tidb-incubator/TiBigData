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

package io.tidb.bigdata.flink.connector.sink.function;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.tikv.common.row.Row;

public class TiDBKeyedProcessFunctionFactory {

  public static KeyedProcessFunction<List<Object>, Row, Row> createKeyedProcessFunction(
      TiDBSinkOptions sinkOptions, DataStream<?> dataStream, List<String> uniqueIndexColumnNames) {
    switch (sinkOptions.getSinkTransaction()) {
      case GLOBAL:
        return new TiDBBatchKeyedProcessFunction(sinkOptions, uniqueIndexColumnNames);
      case CHECKPOINT:
        return new TiDBStreamKeyedProcessFunction(
            dataStream.getExecutionEnvironment().getCheckpointConfig(),
            sinkOptions,
            uniqueIndexColumnNames);
      default:
        throw new IllegalStateException(
            "Can not create KeyedProcessFunction by transaction: "
                + sinkOptions.getSinkTransaction());
    }
  }
}
