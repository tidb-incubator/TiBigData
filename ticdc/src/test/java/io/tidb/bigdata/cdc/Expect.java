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

import io.tidb.bigdata.cdc.RowColumn.Type;
import java.util.function.BiConsumer;
import org.junit.Assert;

public class Expect {

  public static class DDL {

    public final DDLValue.Type type;
    public final String schema;
    public final String table;
    public final String query;

    public DDL(final DDLValue.Type type, final String schema, final String table,
        final String query) {
      this.type = type;
      this.schema = schema;
      this.table = table;
      this.query = query;
    }
  }

  public static class RowChange {

    public final RowChangedValue.Type type;
    public final String schema;
    public final String table;

    public RowChange(final RowChangedValue.Type type, final String schema,
        final String table) {
      this.type = type;
      this.schema = schema;
      this.table = table;
    }
  }

  public static class ColumnData {

    public final String name;
    public final Object value;
    public final Type type;
    public final Class<?> javaType;
    public final long flags;
    public final BiConsumer<RowColumn, Object>[] asTests;

    @SuppressWarnings("unchecked")
    public ColumnData(final String name, final Object value, final Type type,
        final Class<?> javaType, final long flags,
        final BiConsumer<RowColumn, Object>... asTests) {
      this.name = name;
      this.value = value;
      this.type = type;
      this.javaType = javaType;
      this.flags = flags;
      this.asTests = asTests;
    }

    public static void asSmallIntTest(final RowColumn column, final Object expected) {
      Assert.assertEquals(Type.SMALLINT.coerce(expected), column.asSmallInt());
    }

    public static void asIntTest(final RowColumn column, final Object expected) {
      Assert.assertEquals(Type.INT.coerce(expected), column.asInt());
    }

    public static void asBigIntTest(final RowColumn column, final Object expected) {
      Assert.assertEquals(Type.BIGINT.coerce(expected), column.asBigInt());
    }

    public void verify(final RowColumn column) {
      Assert.assertEquals(name, column.getName());
      Assert.assertEquals(value, column.getValue());
      Assert.assertEquals(type, column.getType());
      Assert.assertEquals(javaType, column.getJavaType());
      Assert.assertEquals(flags, column.getFlags());
      for (final BiConsumer<RowColumn, Object> asTest : asTests) {
        asTest.accept(column, value);
      }
    }
  }
}