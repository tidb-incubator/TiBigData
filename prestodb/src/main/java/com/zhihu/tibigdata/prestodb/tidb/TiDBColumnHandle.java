/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.prestodb.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.RecordCursorReader;
import com.zhihu.tibigdata.tidb.ColumnHandleInternal;
import com.zhihu.tibigdata.tidb.DataTypes;
import com.zhihu.tibigdata.tidb.Expressions;
import java.util.List;
import java.util.Objects;
import org.tikv.common.expression.Expression;
import org.tikv.common.types.DataType;

public final class TiDBColumnHandle implements ColumnHandle {

  private final TypeHelper typeHelper;
  private final String name;
  private final String type;
  private final int ordinalPosition;

  @JsonCreator
  public TiDBColumnHandle(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("ordinalPosition") int ordinalPosition) {
    this(requireNonNull(name, "name is null"), requireNonNull(type, "type is null"),
        DataTypes.deserialize(type), ordinalPosition);
  }

  private TiDBColumnHandle(String name, String type, DataType dataType, int ordinalPosition) {
    this.name = name;
    this.type = type;
    this.typeHelper = TypeHelpers.getHelper(dataType).orElseThrow(IllegalStateException::new);
    this.ordinalPosition = ordinalPosition;
  }

  public TiDBColumnHandle(ColumnHandleInternal handle) {
    this(requireNonNull(handle, "handle is null").getName(), DataTypes.serialize(handle.getType()),
        handle.getType(), handle.getOrdinalPosition());
  }

  static List<ColumnHandleInternal> internalHandles(List<TiDBColumnHandle> columns) {
    return columns.stream().map(TiDBColumnHandle::createInternal).collect(toImmutableList());
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getType() {
    return type;
  }

  public Type getPrestoType() {
    return typeHelper.getPrestoType();
  }

  public DataType getTiDBType() {
    return typeHelper.getTiDBType();
  }

  public TypeHelper getTypeHelper() {
    return typeHelper;
  }

  public RecordCursorReader getCursorReader() {
    return getTypeHelper().getReader();
  }

  public Expression createConstantExpression(Object from) {
    return Expressions.constant(getTypeHelper().toTiDB(from), getTiDBType());
  }

  public Expression createColumnExpression() {
    return Expressions.column(getName());
  }

  ColumnHandleInternal createInternal() {
    return new ColumnHandleInternal(getName(), getTiDBType(), getOrdinalPosition());
  }

  @JsonProperty
  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, typeHelper, ordinalPosition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TiDBColumnHandle other = (TiDBColumnHandle) obj;
    return Objects.equals(this.typeHelper, other.typeHelper)
        && Objects.equals(this.name, other.name)
        && Objects.equals(this.ordinalPosition, other.ordinalPosition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", getName())
        .add("prestoType", getPrestoType())
        .add("tidbType", getTiDBType())
        .add("ordinalPosition", getOrdinalPosition())
        .toString();
  }
}
