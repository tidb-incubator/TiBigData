package io.tidb.bigdata.flink.connector.sink;

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
        return new TiDBStreamKeyedProcessFunction(dataStream.getExecutionEnvironment()
            .getCheckpointConfig(), sinkOptions, uniqueIndexColumnNames);
      default:
        throw new IllegalStateException("Can not create KeyedProcessFunction by transaction: "
            + sinkOptions.getSinkTransaction());
    }

  }

}
