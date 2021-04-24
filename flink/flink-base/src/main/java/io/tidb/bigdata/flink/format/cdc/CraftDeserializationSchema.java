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

package io.tidb.bigdata.flink.format.cdc;

import static org.apache.flink.util.Preconditions.checkNotNull;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.RowChangedValue;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.cdc.craft.CraftEventDecoder;
import io.tidb.bigdata.cdc.craft.CraftParser;
import io.tidb.bigdata.cdc.craft.CraftParserState;
import io.tidb.bigdata.flink.format.cdc.RowColumnConverters.Converter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

public class CraftDeserializationSchema implements DeserializationSchema<RowData> {

  private static final long serialVersionUID = 1L;
  /**
   * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
   */
  private final boolean ignoreParseErrors;
  /**
   * TypeInformation of the produced {@link RowData}.
   */
  private final TypeInformation<RowData> resultTypeInfo;

  /**
   * Only read changelogs from the specific schema.
   */
  @Nullable
  private final String schema;
  /**
   * Only read changelogs from the specific table.
   */
  @Nullable
  private final String table;
  /**
   * Number of fields.
   */
  private final int fieldCount;
  private final ParserFactory<CraftParser, CraftParserState> parserFactory;
  private final Map<String, ColumnContext> columns;

  private CraftDeserializationSchema(
      final RowType rowType,
      final TypeInformation<RowData> resultTypeInfo,
      @Nullable final String schema,
      @Nullable final String table,
      final boolean ignoreParseErrors,
      final ParserFactory<CraftParser, CraftParserState> parserFactory) {
    this.resultTypeInfo = checkNotNull(resultTypeInfo);
    this.schema = schema;
    this.table = table;
    this.ignoreParseErrors = ignoreParseErrors;
    this.fieldCount = rowType.getFieldCount();
    this.parserFactory = parserFactory;
    int index = 0;
    this.columns = new HashMap<>();
    for (final RowType.RowField field : rowType.getFields()) {
      columns.put(field.getName(), new ColumnContext(index++, field));
    }
  }

  /**
   * Creates A builder for building a {@link CraftDeserializationSchema}.
   */
  public static CraftDeserializationSchema.Builder builder(final RowType rowType,
      final TypeInformation<RowData> resultTypeInfo) {
    return new CraftDeserializationSchema.Builder(rowType, resultTypeInfo);
  }

  // ------------------------------------------------------------------------------------------
  // Builder
  // ------------------------------------------------------------------------------------------

  private boolean accept(final Event event) {
    switch (event.getType()) {
      case RESOLVED:
        // FALLTHROUGH
      case DDL:
        return false;
      default:
        break;
    }
    if (schema != null) {
      if (!event.getSchema().equals(schema)) {
        return false;
      }
    }
    if (table != null) {
      if (!event.getTable().equals(table)) {
        return false;
      }
    }
    return true;
  }

  private Object[] convert(final RowColumn[] columns) {
    final Object[] objects = new Object[this.columns.size()];
    for (final RowColumn column : columns) {
      final ColumnContext ctx = this.columns.get(column.getName());
      if (ctx == null) {
        continue;
      }
      objects[ctx.index] = ctx.converter.convert(column);
    }
    return objects;
  }

  // ------------------------------------------------------------------------------------------
  private void collectEvent(final Event event, final Collector<RowData> out) {
    if (!accept(event)) {
      return;
    }
    final RowChangedValue value = event.asRowChanged();
    final RowChangedValue.Type type = value.getType();

    Object[] data = null;
    Object[] data2 = null;
    RowKind kind = null;
    RowKind kind2 = null;
    switch (type) {
      case DELETE:
        kind = RowKind.DELETE;
        data = convert(value.getOldValue());
        break;
      case INSERT:
        kind = RowKind.INSERT;
        data = convert(value.getNewValue());
        break;
      case UPDATE:
        kind = RowKind.UPDATE_BEFORE;
        data = convert(value.getOldValue());
        kind2 = RowKind.UPDATE_AFTER;
        data2 = convert(value.getNewValue());
        break;
      default:
        if (!ignoreParseErrors) {
          throw new IllegalArgumentException("Unknown row changed event " + type);
        }
    }
    out.collect(GenericRowData.ofKind(kind, data));
    if (data2 != null) {
      out.collect(GenericRowData.ofKind(kind2, data2));
    }
  }

  @Override
  public void deserialize(final byte[] message, final Collector<RowData> out) throws IOException {
    try {
      for (final Event event : new CraftEventDecoder(message, parserFactory.createParser())) {
        collectEvent(event, out);
      }
    } catch (Throwable throwable) {
      if (!ignoreParseErrors) {
        throw new IOException("Corrupted TiCDC craft message.", throwable);
      }
    }
  }

  @Override
  public RowData deserialize(final byte[] message) throws IOException {
    throw new IllegalStateException("A collector is required for deserializing.");
  }

  @Override
  public boolean isEndOfStream(final RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return resultTypeInfo;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CraftDeserializationSchema that = (CraftDeserializationSchema) o;
    return ignoreParseErrors == that.ignoreParseErrors
        && fieldCount == that.fieldCount
        && Objects.equals(resultTypeInfo, that.resultTypeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resultTypeInfo, ignoreParseErrors, fieldCount);
  }

  private static class ColumnContext implements Serializable {

    private final int index;
    private final RowField field;
    private final Converter converter;

    private ColumnContext(final int index, final RowField field) {
      this.index = index;
      this.field = field;
      this.converter = RowColumnConverters.createConverter(field.getType());
    }
  }

  /**
   * A builder for creating a {@link CraftDeserializationSchema}.
   */
  @Internal
  public static final class Builder {

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;
    private final ParserFactory<CraftParser, CraftParserState> parserFactory;
    private String schema = null;
    private String table = null;
    private boolean ignoreParseErrors = false;

    private Builder(final RowType rowType, final TypeInformation<RowData> resultTypeInfo) {
      this.rowType = rowType;
      this.resultTypeInfo = resultTypeInfo;
      this.parserFactory = ParserFactory.craft();
    }

    public CraftDeserializationSchema.Builder setSchema(final String schema) {
      this.schema = schema;
      return this;
    }

    public CraftDeserializationSchema.Builder setTable(final String table) {
      this.table = table;
      return this;
    }

    public CraftDeserializationSchema.Builder setIgnoreParseErrors(
        final boolean ignoreParseErrors) {
      this.ignoreParseErrors = ignoreParseErrors;
      return this;
    }

    public CraftDeserializationSchema build() {
      return new CraftDeserializationSchema(rowType, resultTypeInfo, schema, table,
          ignoreParseErrors, parserFactory);
    }
  }
}
