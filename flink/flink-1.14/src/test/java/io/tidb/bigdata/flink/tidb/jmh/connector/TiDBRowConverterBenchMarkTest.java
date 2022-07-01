package io.tidb.bigdata.flink.tidb.jmh.connector;

import io.tidb.bigdata.flink.connector.utils.TiDBRowConverter;
import io.tidb.bigdata.tidb.meta.CIStr;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.BitType;
import io.tidb.bigdata.tidb.types.IntegerType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.Ignore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
public class TiDBRowConverterBenchMarkTest {

  public TiDBRowConverter tiDBRowConverter;
  public GenericRowData rowData;

  @Setup(Level.Trial)
  public void setup() {
    List<TiColumnInfo> columns = new ArrayList<>();
    /**
     * "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n" + "(\n" + " c1 int(11) NOT NULL,\n" + " c2 bit NOT
     * NULL,\n" + " c3 tinyint NOT NULL,\n" + " c4 smallint NOT NULL,\n" + " c5 mediumint NOT
     * NULL,\n" + " c6 year NOT NULL,\n" + " PRIMARY KEY (`c1`)\n" + ")";
     */
    TiColumnInfo col1 = new TiColumnInfo(1, "c1", 0, IntegerType.INT, true);
    TiColumnInfo col2 = new TiColumnInfo(2, "c2", 1, BitType.BIT, false);
    TiColumnInfo col3 = new TiColumnInfo(3, "c3", 2, IntegerType.TINYINT, false);
    TiColumnInfo col4 = new TiColumnInfo(4, "c4", 3, IntegerType.SMALLINT, false);
    TiColumnInfo col5 = new TiColumnInfo(5, "c5", 4, IntegerType.MEDIUMINT, false);
    TiColumnInfo col6 = new TiColumnInfo(6, "c6", 5, IntegerType.YEAR, false);
    columns.add(col1);
    columns.add(col2);
    columns.add(col3);
    columns.add(col4);
    columns.add(col5);
    columns.add(col6);

    TiTableInfo tiTableInfo =
        new TiTableInfo(
            1,
            CIStr.newCIStr("t1"),
            "",
            "",
            true,
            false,
            columns,
            null,
            "",
            0,
            0,
            0,
            0,
            null,
            null,
            null,
            0,
            0,
            0,
            null,
            0);

    tiDBRowConverter = new TiDBRowConverter(tiTableInfo);

    rowData = new GenericRowData(RowKind.INSERT, 6);

    rowData.setField(0, 1);
    rowData.setField(1, true);
    rowData.setField(2, (byte) 1);
    rowData.setField(3, (short) 1);
    rowData.setField(4, 1);
    rowData.setField(5, 1995);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public Row toTiRow() {
    return tiDBRowConverter.toTiRow(rowData, false);
  }

  @Ignore
  public void test() throws RunnerException {

    Options opt =
        new OptionsBuilder().include(TiDBRowConverterBenchMarkTest.class.getSimpleName()).build();
    new Runner(opt).run();
  }
}
