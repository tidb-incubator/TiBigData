package io.tidb.bigdata.tidb.key;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.tidb.bigdata.tidb.codec.CodecDataOutput;
import io.tidb.bigdata.tidb.handle.CommonHandle;
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.IntHandle;
import io.tidb.bigdata.tidb.meta.CIStr;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.ObjectRowImpl;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.types.IntegerType;
import io.tidb.bigdata.tidb.types.StringType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.tikv.common.util.Pair;

public class IndexKeyTest {

  @Test
  public void encodeIndexDataCommandHandleValuesTest() {
    TiColumnInfo col1 = new TiColumnInfo(1, "a", 0, IntegerType.BIGINT, false);
    TiColumnInfo col2 = new TiColumnInfo(2, "b", 1, StringType.VARCHAR, true);
    List<TiColumnInfo> tableColumns = new ArrayList<>();
    tableColumns.add(col1);
    tableColumns.add(col2);
    TiTableInfo tableInfo =
        new TiTableInfo(
            1,
            CIStr.newCIStr("test"),
            "",
            "",
            false,
            true,
            1,
            tableColumns,
            null,
            "",
            0,
            0,
            0,
            0,
            null,
            null,
            null,
            1,
            1,
            0,
            null,
            0);

    TiIndexColumn index1 = new TiIndexColumn(CIStr.newCIStr("a"), 0, DataType.UNSPECIFIED_LEN);
    List<TiIndexColumn> indexColumns1 = new ArrayList<>();
    indexColumns1.add(index1);
    TiIndexInfo indexInfo1 =
        new TiIndexInfo(
            1,
            CIStr.newCIStr("test"),
            CIStr.newCIStr("test"),
            indexColumns1,
            true,
            false,
            0,
            "",
            0,
            false,
            true);

    ArrayList<Object[]> testRows = new ArrayList<>();
    ArrayList<Pair<Boolean, byte[]>> expectations = new ArrayList<>();

    Object[] row1 = new Object[] {1, "1"};
    CodecDataOutput codecDataOutputRow1 = new CodecDataOutput();
    codecDataOutputRow1.write(
        IndexKey.toIndexKey(
                tableInfo.getId(), indexInfo1.getId(), TypedKey.toTypedKey(1, IntegerType.BIGINT))
            .getBytes());
    testRows.add(row1);
    expectations.add(new Pair<>(true, codecDataOutputRow1.toBytes()));

    Object[] row2 = new Object[] {null, "2"};
    CodecDataOutput codecDataOutputRow2 = new CodecDataOutput();
    codecDataOutputRow2.write(
        IndexKey.toIndexKey(
                tableInfo.getId(),
                indexInfo1.getId(),
                TypedKey.toTypedKey(null, IntegerType.BIGINT))
            .getBytes());
    codecDataOutputRow2.write(
        CommonHandle.newCommonHandle(new DataType[] {StringType.VARCHAR}, new Object[] {"2"})
            .encodedAsKey());
    testRows.add(row2);
    expectations.add(new Pair<>(false, codecDataOutputRow2.toBytes()));

    for (int i = 0; i < testRows.size(); i++) {
      Row row = ObjectRowImpl.create(testRows.get(i));
      Handle handle =
          CommonHandle.newCommonHandle(
              new DataType[] {StringType.VARCHAR}, new Object[] {row.get(1, StringType.VARCHAR)});
      IndexKey.EncodeIndexDataResult result =
          IndexKey.genIndexKey(1, row, indexInfo1, handle, tableInfo);
      assertEquals(expectations.get(i).first, result.distinct);
      assertArrayEquals(expectations.get(i).second, result.indexKey);
    }
  }

  @Test
  public void encodeIndexDataIntHandleValuesTest() {
    TiColumnInfo col1 = new TiColumnInfo(1, "a", 0, IntegerType.BIGINT, false);
    TiColumnInfo col2 = new TiColumnInfo(2, "b", 1, IntegerType.BIGINT, true);
    List<TiColumnInfo> tableColumns = new ArrayList<>();
    tableColumns.add(col1);
    tableColumns.add(col2);
    TiTableInfo tableInfo =
        new TiTableInfo(
            1,
            CIStr.newCIStr("test"),
            "",
            "",
            false,
            true,
            1,
            tableColumns,
            null,
            "",
            0,
            0,
            0,
            0,
            null,
            null,
            null,
            1,
            1,
            0,
            null,
            0);

    TiIndexColumn index1 = new TiIndexColumn(CIStr.newCIStr("a"), 0, DataType.UNSPECIFIED_LEN);
    List<TiIndexColumn> indexColumns1 = new ArrayList<>();
    indexColumns1.add(index1);
    TiIndexInfo indexInfo1 =
        new TiIndexInfo(
            1,
            CIStr.newCIStr("test"),
            CIStr.newCIStr("test"),
            indexColumns1,
            true,
            false,
            0,
            "",
            0,
            false,
            true);

    ArrayList<Object[]> testRows = new ArrayList<>();
    ArrayList<Pair<Boolean, byte[]>> expectations = new ArrayList<>();

    Object[] row1 = new Object[] {1, 1};
    CodecDataOutput codecDataOutputRow1 = new CodecDataOutput();
    codecDataOutputRow1.write(
        IndexKey.toIndexKey(
                tableInfo.getId(), indexInfo1.getId(), TypedKey.toTypedKey(1, IntegerType.BIGINT))
            .getBytes());
    testRows.add(row1);
    expectations.add(new Pair<>(true, codecDataOutputRow1.toBytes()));

    Object[] row2 = new Object[] {null, 2};
    CodecDataOutput codecDataOutputRow2 = new CodecDataOutput();
    codecDataOutputRow2.write(
        IndexKey.toIndexKey(
                tableInfo.getId(),
                indexInfo1.getId(),
                TypedKey.toTypedKey(null, IntegerType.BIGINT),
                TypedKey.toTypedKey(2, IntegerType.BIGINT))
            .getBytes());
    testRows.add(row2);
    expectations.add(new Pair<>(false, codecDataOutputRow2.toBytes()));

    for (int i = 0; i < testRows.size(); i++) {
      Row row = ObjectRowImpl.create(testRows.get(i));
      Handle handle = new IntHandle(((Number) row.get(1, IntegerType.BIGINT)).longValue());
      IndexKey.EncodeIndexDataResult result =
          IndexKey.genIndexKey(1, row, indexInfo1, handle, tableInfo);
      assertEquals(expectations.get(i).first, result.distinct);
      assertArrayEquals(expectations.get(i).second, result.indexKey);
    }
  }
}
