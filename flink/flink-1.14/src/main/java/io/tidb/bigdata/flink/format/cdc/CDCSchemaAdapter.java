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

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.flink.format.cdc.RowColumnConverters.Converter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

public class CDCSchemaAdapter implements Serializable {

  /** Readable metadata */
  private final CDCMetadata[] metadata;

  private static class ColumnContext implements Serializable {

    private final int index;
    private final Converter converter;

    private ColumnContext(final int index, final LogicalType type) {
      this.index = index;
      this.converter = RowColumnConverters.createConverter(type);
    }
  }

  public static class RowBuilder {

    private final Object[] objects;

    public RowBuilder(Object[] objects) {
      this.objects = objects;
    }

    public GenericRowData build(RowKind kind) {
      return GenericRowData.ofKind(kind, objects);
    }

    public GenericRowData insert() {
      return build(RowKind.INSERT);
    }

    public GenericRowData delete() {
      return build(RowKind.DELETE);
    }

    public GenericRowData updateAfter() {
      return build(RowKind.UPDATE_AFTER);
    }

    public GenericRowData updateBefore() {
      return build(RowKind.UPDATE_BEFORE);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(objects);
    }

    @Override
    public boolean equals(Object rhs) {
      if (this == rhs) {
        return true;
      }
      if (!(rhs instanceof RowBuilder)) {
        return false;
      }
      return Objects.deepEquals(objects, ((RowBuilder) rhs).objects);
    }
  }

  private final Map<String, ColumnContext> physicalFields;

  /** Number of physical fields. */
  private final int physicalFieldCount;

  private final DataType physicalDataType;

  public CDCSchemaAdapter(final DataType physicalDataType, CDCMetadata[] metadata) {
    this.metadata = CDCMetadata.notNull(metadata);
    this.physicalDataType = physicalDataType;
    final RowType physicalRowType = (RowType) physicalDataType.getLogicalType();
    this.physicalFieldCount = physicalRowType.getFieldCount();
    this.physicalFields = new HashMap<>();
    int index = 0;
    for (final RowField field : physicalRowType.getFields()) {
      physicalFields.put(field.getName(), new ColumnContext(index++, field.getType()));
    }
  }

  public RowBuilder convert(final Event event) {
    return new RowBuilder(makeRow(event));
  }

  public RowBuilder convert(final Event event, final RowColumn[] columns) {
    final Object[] objects = makeRow(event);
    for (final RowColumn column : columns) {
      final ColumnContext ctx = physicalFields.get(column.getName());
      if (ctx == null) {
        continue;
      }
      objects[ctx.index] = ctx.converter.convert(column);
    }
    return new RowBuilder(objects);
  }

  private Object[] makeRow(final Event event) {
    Object[] objects = new Object[physicalFieldCount + metadata.length];
    for (int i = physicalFieldCount; i <= physicalFieldCount + metadata.length - 1; i++) {
      objects[i] = metadata[i - physicalFieldCount].extract(event);
    }
    return objects;
  }

  public TypeInformation<RowData> getProducedType() {
    return getProducedType(physicalDataType, metadata);
  }

  @SuppressWarnings("unchecked")
  public static TypeInformation<RowData> getProducedType(
      DataType physicalDataType, CDCMetadata[] metadata) {
    List<Field> fields =
        Arrays.stream(metadata)
            .map(meta -> DataTypes.FIELD(meta.getKey(), meta.getType()))
            .collect(Collectors.toList());
    DataType dataType = DataTypeUtils.appendRowFields(physicalDataType, fields);
    return (TypeInformation<RowData>)
        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(dataType);
  }
}
