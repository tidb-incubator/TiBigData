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

package io.tidb.bigdata.flink.tidb.jmh.connector;

import java.util.concurrent.TimeUnit;
import org.junit.Ignore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
public class ParseLongBenchMarkTest {

  @Param({"1", "1000000"})
  public int data;

  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @Fork(1)
  @Warmup(iterations = 2)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void object2LongWithParseLong(Blackhole blackhole) {
    Object obj = new Integer(data);
    long ans = Long.parseLong(obj.toString());
    blackhole.consume(ans);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @Fork(1)
  @Warmup(iterations = 2)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void object2LongWithNumber(Blackhole blackhole) {
    Object obj = new Integer(data);
    Number number = (Number) obj;
    long ans = number.longValue();
    blackhole.consume(ans);
  }

  @Ignore
  public void test() throws RunnerException {
    Options opt =
        new OptionsBuilder().include(ParseLongBenchMarkTest.class.getSimpleName()).build();
    new Runner(opt).run();
  }
}
