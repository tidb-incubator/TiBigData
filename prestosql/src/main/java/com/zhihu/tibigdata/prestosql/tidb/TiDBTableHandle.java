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

package com.zhihu.tibigdata.prestosql.tidb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zhihu.tibigdata.tidb.TableHandleInternal;
import com.zhihu.tibigdata.tidb.Wrapper;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;

public final class TiDBTableHandle extends Wrapper<TableHandleInternal> implements
    ConnectorTableHandle, ConnectorOutputTableHandle, ConnectorInsertTableHandle {

  @JsonCreator
  public TiDBTableHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName) {
    super(new TableHandleInternal(connectorId, schemaName, tableName));
  }

  TiDBTableHandle(TableHandleInternal internal) {
    super(internal);
  }

  @JsonProperty
  public String getConnectorId() {
    return getInternal().getConnectorId();
  }

  @JsonProperty
  public String getSchemaName() {
    return getInternal().getSchemaName();
  }

  @JsonProperty
  public String getTableName() {
    return getInternal().getTableName();
  }
}
