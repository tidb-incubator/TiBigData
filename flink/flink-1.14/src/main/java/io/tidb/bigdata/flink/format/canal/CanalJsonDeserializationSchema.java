/*
 * Copyright 2023 TiDB Project Authors.
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

package io.tidb.bigdata.flink.format.canal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class CanalJsonDeserializationSchema extends CanalDeserializationSchema {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public CanalJsonDeserializationSchema(
      DataType physicalDataType,
      Set<String> schemas,
      Set<String> tables,
      CDCMetadata[] metadata,
      long startTs,
      TypeInformation<RowData> producedTypeInfo,
      boolean ignoreParseErrors) {
    super(
        physicalDataType, schemas, tables, metadata, startTs, producedTypeInfo, ignoreParseErrors);
  }

  @Override
  public List<FlatMessage> decodeToFlatMessage(byte[] data) throws Exception {
    return ImmutableList.of(MAPPER.readValue(data, FlatMessage.class));
  }
}
