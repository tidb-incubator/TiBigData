package io.tidb.bigdata.flink.connector.sink;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.row.Row;

public class TiDBBatchKeyedProcessFunction extends KeyedProcessFunction<List<Object>, Row, Row> {

  private final TiDBSinkOptions sinkOptions;
  private final List<String> uniqueIndexColumnNames;

  private ValueState<Boolean> existState;

  public TiDBBatchKeyedProcessFunction(TiDBSinkOptions sinkOptions,
      List<String> uniqueIndexColumnNames) {
    this.sinkOptions = sinkOptions;
    this.uniqueIndexColumnNames = uniqueIndexColumnNames;
  }


  @Override
  public void open(Configuration parameters) throws Exception {

    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
        .updateTtlOnCreateAndWrite()
        .disableCleanupInBackground()
        .build();

    ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
        "exist-state-" + String.join("-", uniqueIndexColumnNames),
        Boolean.class
    );
    existStateDesc.enableTimeToLive(stateTtlConfig);
    existState = this.getRuntimeContext().getState(existStateDesc);
  }

  @Override
  public void processElement(Row row, KeyedProcessFunction<List<Object>, Row, Row>.Context ctx,
      Collector<Row> out) throws Exception {
    if (existState.value() == null) {
      existState.update(true);
      out.collect(row);
    } else if (!sinkOptions.isDeduplicate()) {
      String names = String.join(", ", uniqueIndexColumnNames);
      String values = ctx.getCurrentKey().stream().map(Object::toString)
          .collect(Collectors.joining(", "));
      throw new IllegalStateException(String.format(
          "Duplicate index in one batch, please enable deduplicate, index: [%s] = [%s]", names,
          values));
    }

  }
}
