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

package io.tidb.bigdata.cdc;

import static io.tidb.bigdata.cdc.FileLoader.getFile;
import static io.tidb.bigdata.cdc.FileLoader.getFileContent;
import static io.tidb.bigdata.cdc.RowColumn.Type;

import io.tidb.bigdata.cdc.json.JsonValueDecoder;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.function.BiConsumer;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class RowColumnTest extends TestCase {

  private static final Expect.ColumnData[] expectedRowData = new Expect.ColumnData[]{
      new Expect.ColumnData("c1", (byte) 1, Type.TINYINT, Byte.class, 0,
          Expect.ColumnData::asSmallIntTest, Expect.ColumnData::asIntTest,
          Expect.ColumnData::asBigIntTest),
  };
  private byte[] value;

  private static byte[] encodeValue(final byte[] json) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);
    dos.writeLong(json.length);
    dos.write(json);
    dos.close();
    return baos.toByteArray();
  }

  private static CoercionTest createNumberCoercionTest(final String input, final Class from,
      final Type type) {
    return Misc.uncheckedRun(() -> {
      Method fromValueOf = from.getMethod("valueOf", String.class);
      Method toValueOf = type.getJavaType().getMethod("valueOf", String.class);
      return new CoercionTest(fromValueOf.invoke(null, input), toValueOf.invoke(null, input),
          type);
    });
  }

  private static CoercionTest[] createNumberCoercionTests() {
    final Class[] numberJavaTypes = new Class[]{Long.class, Integer.class, Short.class, Byte.class,
        Float.class, Double.class};
    final Type[] numberTypes = new Type[]{Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT,
        Type.FLOAT,
        Type.DOUBLE, Type.YEAR, Type.BIT};
    final ArrayList<CoercionTest> tests = new ArrayList<>();
    for (final Class javaType : numberJavaTypes) {
      for (final Type type : numberTypes) {
        tests.add(createNumberCoercionTest("1", javaType, type));
      }
    }
    return tests.toArray(new CoercionTest[0]);
  }

  private static CoercionTest[] createToNullCoercionTests() {
    return new CoercionTest[]{
        new CoercionTest("1", null, Type.NULL),
        new CoercionTest(1, null, Type.NULL),
        new CoercionTest(1.0, null, Type.NULL),
        new CoercionTest(true, null, Type.NULL),
    };
  }

  private static CoercionTest[] createBinaryCoercionTests() {
    final String encoded = "5rWL6K+VdGV4dA==";
    final byte[] decoded = "测试text".getBytes(StandardCharsets.UTF_8);
    return Arrays.stream(new Type[]{Type.BINARY, Type.VARBINARY,
        Type.BLOB, Type.TINYBLOB, Type.MEDIUMBLOB, Type.LONGBLOB})
        .map(t -> new CoercionTest(Base64.getDecoder().decode(encoded), decoded, t))
        .toArray(CoercionTest[]::new);
  }

  private static CoercionTest[] createStringCoercionTests() {
    final String from = "test";
    final String to = "test";
    return Arrays.stream(new Type[]{Type.VARCHAR, Type.JSON})
        .map(t -> new CoercionTest(from, to, t)).toArray(CoercionTest[]::new);
  }

  private static LocalDate toLocalDate(final Object o) {
    return ((TemporalAccessor) o).query(TemporalQueries.localDate());
  }

  private static LocalTime toLocalTime(final Object o) {
    return ((TemporalAccessor) o).query(TemporalQueries.localTime());
  }

  private static LocalDateTime toLocalDateTime(final Object o) {
    final TemporalAccessor accessor = (TemporalAccessor) o;
    return LocalDateTime.of(accessor.query(TemporalQueries.localDate()),
        accessor.query(TemporalQueries.localTime()));
  }

  private static CoercionTest[] createFromStringCoercionTests() {
    return new CoercionTest[]{
        new CoercionTest("1973-12-30 15:30:00",
            LocalDateTime.of(1973, 12, 30, 15, 30),
            Type.TIMESTAMP, (l, r) -> Assert.assertEquals(toLocalDateTime(l), toLocalDateTime(r))
        ),
        new CoercionTest("2015-12-20 23:58:58",
            LocalDateTime.of(2015, 12, 20, 23, 58, 58),
            Type.DATETIME, (l, r) -> Assert.assertEquals(toLocalDateTime(l), toLocalDateTime(r))
        ),
        new CoercionTest("2000-01-01", LocalDate.of(2000, 1, 1),
            Type.DATE, (l, r) -> Assert.assertEquals(toLocalDate(l), toLocalDate(r))
        ),
        new CoercionTest("23:59:59", LocalTime.of(23, 59, 59),
            Type.TIME, (l, r) -> Assert.assertEquals(toLocalTime(l), toLocalTime(r))
        ),
    };
  }

  @Override
  public void setUp() {
    value = Misc.uncheckedRun(() -> encodeValue(getFileContent(getFile("json", "row.json", true))));
  }

  @Override
  public void tearDown() {
  }

  @Test
  public void testDecode() {
    for (final JsonValueDecoder it = new JsonValueDecoder(value,
        ParserFactory.json().createParser());
        it.hasNext(); ) {
      final RowInsertedValue insert = (RowInsertedValue) it.next();
      Assert.assertEquals(insert.getNewValue().length, 27);
      final RowColumn[] columns = insert.getNewValue();
      for (int idx = 0, size = expectedRowData.length; idx < size; ++idx) {
        expectedRowData[idx].verify(columns[idx]);
      }
    }
  }

  private void runCoercionTests(final CoercionTest[] coercionTests) {
    for (final CoercionTest test : coercionTests) {
      test.verify();
    }
  }

  @Test
  public void testTypeCoercion() {
    runCoercionTests(createNumberCoercionTests());
    runCoercionTests(createToNullCoercionTests());
    runCoercionTests(createBinaryCoercionTests());
    runCoercionTests(createStringCoercionTests());
    runCoercionTests(createFromStringCoercionTests());
  }

  private static class CoercionTest {

    final Object from;
    final Object to;
    final Type type;
    final BiConsumer<Object, Object> verify;

    public CoercionTest(final Object from, final Object to, final Type type) {
      this(from, to, type, CoercionTest::verify);
    }

    public CoercionTest(final Object from, final Object to, final Type type,
        final BiConsumer<Object, Object> verify) {
      this.from = from;
      this.to = to;
      this.type = type;
      this.verify = verify;
    }

    private static void verify(final Object o1, final Object o2) {
      if (o1 != null && o1.getClass().isArray()) {
        if (o2.getClass().isArray()) {
          final int length = Array.getLength(o1);
          Assert.assertEquals(length, Array.getLength(o2));
          for (int idx = 0; idx < length; ++idx) {
            Assert.assertEquals(Array.get(o1, idx), Array.get(o2, idx));
          }
        } else {
          Assert.fail();
        }
      } else {
        Assert.assertEquals(o1, o2);
      }
    }

    public void verify() {
      this.verify.accept(to, type.coerce(from));
    }
  }
}
