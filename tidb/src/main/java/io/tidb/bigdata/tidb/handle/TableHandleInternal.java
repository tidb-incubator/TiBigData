/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.tidb.handle;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Joiner;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Objects;

public class TableHandleInternal implements Serializable {

  private final String connectorId;
  private final String schemaName;
  private final TiTableInfo tiTableInfo;
  private final String tableName;
  // If we need to encode TiTableInfo to json, we could use base64 to encode it,
  // since TiTableInfo use tikv shaded jackson package and can not import jackson time module.
  // Otherwise, ignore this field.
  // TODO: use it in Presto/Trino API.
  private final String tiTableInfoBase64String;

  public TableHandleInternal(String connectorId, String schemaName, TiTableInfo tiTableInfo) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.schemaName = requireNonNull(schemaName, "schemaName is null");
    this.tiTableInfo = requireNonNull(tiTableInfo, "tiTableInfo can not be null");
    this.tableName = tiTableInfo.getName();
    this.tiTableInfoBase64String = encodeTiTableInfo(tiTableInfo);
  }

  public TableHandleInternal(String schemaName, TiTableInfo tiTableInfo) {
    this("", schemaName, tiTableInfo);
  }

  public String getSchemaTableName() {
    return Joiner.on(".").join(getSchemaName(), getTableName());
  }

  public String getConnectorId() {
    return connectorId;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public TiTableInfo getTiTableInfo() {
    return tiTableInfo;
  }

  public String getTiTableInfoBase64String() {
    return tiTableInfoBase64String;
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorId, schemaName, tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TableHandleInternal other = (TableHandleInternal) obj;
    return Objects.equals(this.connectorId, other.connectorId)
        && Objects.equals(this.schemaName, other.schemaName)
        && Objects.equals(this.tableName, other.tableName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("connectorId", connectorId)
        .add("schema", schemaName)
        .add("table", tableName)
        .toString();
  }

  public static String encodeTiTableInfo(TiTableInfo tiTableInfo) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(tiTableInfo);
      return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static TiTableInfo decodeTiTableInfo(String base64String) {
    try (ByteArrayInputStream byteArrayInputStream =
            new ByteArrayInputStream(Base64.getDecoder().decode(base64String));
        ObjectInputStream objectOutputStream = new ObjectInputStream(byteArrayInputStream)) {
      return (TiTableInfo) objectOutputStream.readObject();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
