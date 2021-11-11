package io.tidb.bigdata.flink.connector.sink;

import java.util.List;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.row.Row;

public class TiDBBatchKeyedProcessFunction extends KeyedProcessFunction<List<Object>, Row, Row> {

  private ValueState<Boolean> existState;


  @Override
  public void open(Configuration parameters) throws Exception {

    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
        .updateTtlOnCreateAndWrite()
        .disableCleanupInBackground()
        .build();

    ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
        "exist-state",
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
    }

  }
}
