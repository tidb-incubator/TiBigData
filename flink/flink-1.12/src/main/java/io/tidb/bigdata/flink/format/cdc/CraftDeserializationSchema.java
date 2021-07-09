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
import io.tidb.bigdata.cdc.Key.Type;
import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.RowChangedValue;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.cdc.craft.CraftEventDecoder;
import io.tidb.bigdata.cdc.craft.CraftParser;
import io.tidb.bigdata.cdc.craft.CraftParserState;
import io.tidb.bigdata.flink.format.cdc.RowColumnConverters.Converter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
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
   * Only read changelogs from the specific schemas.
   */
  @Nullable
  private final Set<String> schemas;
  /**
   * Only read changelogs from the specific tables.
   */
  @Nullable
  private final Set<String> tables;
  /**
   * Only read changelogs of some specific types.
   */
  @Nullable
  private final Set<Type> types;
  private final long earliestTs;
  /**
   * Number of fields.
   */
  private final int physicalFieldCount;
  private final int producedFieldCount;
  private final ParserFactory<CraftParser, CraftParserState> parserFactory;
  private final Map<String, ColumnContext> columns;
  private final List<ReadableMetadata> requestedMetadata;

  CraftDeserializationSchema(
      final RowType rowType,
      List<ReadableMetadata> requestedMetadata,
      final TypeInformation<RowData> resultTypeInfo,
      final long earliestTs,
      @Nullable final Set<Type> types,
      @Nullable final Set<String> schemas,
      @Nullable final Set<String> tables,
      final boolean ignoreParseErrors) {
    this.requestedMetadata = requestedMetadata;
    this.resultTypeInfo = checkNotNull(resultTypeInfo);
    this.earliestTs = earliestTs;
    this.types = types;
    this.schemas = schemas;
    this.tables = tables;
    this.ignoreParseErrors = ignoreParseErrors;
    this.physicalFieldCount = rowType.getFieldCount();
    this.producedFieldCount = this.physicalFieldCount + requestedMetadata.size();
    this.parserFactory = ParserFactory.craft();
    int index = 0;
    this.columns = new HashMap<>();
    for (final RowField field : rowType.getFields()) {
      columns.put(field.getName(), new ColumnContext(index++, field));
    }
  }

  private boolean acceptEvent(final Event event) {
    if (event.getTs() < earliestTs) {
      // skip not events that have ts older than specific
      return false;
    }
    // skip not interested types
    return types == null || types.contains(event.getType());
  }

  private boolean acceptSchemaAndTable(final Event event) {
    if (schemas != null && !schemas.contains(event.getSchema())) {
      return false;
    }
    return tables == null || tables.contains(event.getTable());
  }

  private Object[] convert(final Event event, final RowColumn[] columns) {
    final Object[] objects = new Object[producedFieldCount];
    for (final RowColumn column : columns) {
      final ColumnContext ctx = this.columns.get(column.getName());
      if (ctx == null) {
        continue;
      }
      objects[ctx.index] = ctx.converter.convert(column);
    }
    return convertMeta(event, objects);
  }

  private Object[] convertMeta(final Event event, Object[] objects) {
    int metaIndex = physicalFieldCount;
    for (final ReadableMetadata metadata : requestedMetadata) {
      objects[metaIndex++] = metadata.extractor.apply(event);
    }
    return objects;
  }

  private void collectRowChanged(final Event event, final Collector<RowData> out) {
    final RowChangedValue value = event.asRowChanged();
    final RowChangedValue.Type type = value.getType();

    Object[] data = null;
    Object[] data2 = null;
    RowKind kind = null;
    RowKind kind2 = null;
    switch (type) {
      case DELETE:
        kind = RowKind.DELETE;
        data = convert(event, value.getOldValue());
        break;
      case INSERT:
        kind = RowKind.INSERT;
        data = convert(event, value.getNewValue());
        break;
      case UPDATE:
        data = convert(event, value.getOldValue());
        data2 = convert(event, value.getNewValue());
        if (Arrays.deepEquals(data, data2)) {
          // All selected data remain the same, skip this event
          return;
        }
        kind = RowKind.UPDATE_BEFORE;
        kind2 = RowKind.UPDATE_AFTER;
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

  private void collectDDL(final Event event, final Collector<RowData> out) {
    out.collect(GenericRowData.ofKind(
        RowKind.INSERT, convertMeta(event, new Object[producedFieldCount])));
  }

  private void collectResolved(final Event event, final Collector<RowData> out) {
    out.collect(GenericRowData.ofKind(
        RowKind.INSERT, convertMeta(event, new Object[producedFieldCount])));
  }

  // ------------------------------------------------------------------------------------------
  private void collectEvent(final Event event, final Collector<RowData> out) {
    if (!acceptEvent(event)) {
      return;
    }
    Type type = event.getType();
    if (type == Type.RESOLVED) {
      // Resolved event is always eligible even though they don't have schema and table specified
      collectResolved(event, out);
      return;
    }
    if (!acceptSchemaAndTable(event)) {
      return;
    }
    switch (type) {
      case ROW_CHANGED:
        collectRowChanged(event, out);
        break;
      case DDL:
        collectDDL(event, out);
        break;
      default:
        throw new IllegalStateException("Unknown event type: " + event.getType());
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
  public RowData deserialize(final byte[] message) {
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
        && physicalFieldCount == that.physicalFieldCount
        && producedFieldCount == that.producedFieldCount
        && Objects.equals(resultTypeInfo, that.resultTypeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resultTypeInfo, ignoreParseErrors, physicalFieldCount, producedFieldCount);
  }

  private static class ColumnContext implements Serializable {

    private final int index;
    private final Converter converter;

    private ColumnContext(final int index, final RowField field) {
      this.index = index;
      this.converter = RowColumnConverters.createConverter(field.getType());
    }
  }
}
