package io.tidb.bigdata.flink.connector.sink;

import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction;
import java.util.List;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.tikv.common.row.Row;

public class TiDBKeyedProcessFunctionFactory {

  public static KeyedProcessFunction<List<Object>, Row, Row> createKeyedProcessFunction(
      TiDBSinkOptions sinkOptions) {
    if (sinkOptions.getSinkTransaction() == SinkTransaction.GLOBAL) {
      return new TiDBBatchKeyedProcessFunction();
    } else {
      return new TiDBStreamKeyedProcessFunction();
    }
  }

}
