/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.catalog;


import static io.tidb.bigdata.tidb.codec.MetaCodec.KEY_DBs;

import com.google.common.collect.ImmutableList;
import org.tikv.shade.com.google.protobuf.ByteString;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.codec.KeyUtils;
import io.tidb.bigdata.tidb.codec.MetaCodec;
import io.tidb.bigdata.tidb.meta.TiDBInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.Snapshot;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.util.Pair;
import org.tikv.shade.com.fasterxml.jackson.core.JsonParseException;
import org.tikv.shade.com.fasterxml.jackson.databind.JsonMappingException;
import org.tikv.shade.com.fasterxml.jackson.databind.ObjectMapper;

public class CatalogTransaction {

  protected static final Logger logger = LoggerFactory.getLogger(CatalogTransaction.class);
  private final Snapshot snapshot;

  CatalogTransaction(Snapshot snapshot) {
    this.snapshot = snapshot;
  }

  public static <T> T parseFromJson(ByteString json, Class<T> cls) {
    Objects.requireNonNull(json, "json is null");
    Objects.requireNonNull(cls, "cls is null");

    logger.debug(String.format("Parse Json %s : %s", cls.getSimpleName(), json.toStringUtf8()));
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json.toStringUtf8(), cls);
    } catch (JsonParseException | JsonMappingException e) {
      String errMsg =
          String.format(
              "Invalid JSON value for Type %s: %s\n", cls.getSimpleName(), json.toStringUtf8());
      throw new TiClientInternalException(errMsg, e);
    } catch (Exception e1) {
      throw new TiClientInternalException("Error parsing Json", e1);
    }
  }

  long getLatestSchemaVersion() {
    ByteString versionBytes = MetaCodec.bytesGet(MetaCodec.KEY_SCHEMA_VERSION, this.snapshot);
    CodecDataInput cdi = new CodecDataInput(versionBytes.toByteArray());
    return Long.parseLong(new String(cdi.toByteArray(), StandardCharsets.UTF_8));
  }

  public List<TiDBInfo> getDatabases() {
    List<Pair<ByteString, ByteString>> fields =
        MetaCodec.hashGetFields(KEY_DBs, this.snapshot);
    ImmutableList.Builder<TiDBInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      builder.add(parseFromJson(pair.second, TiDBInfo.class));
    }
    return builder.build();
  }

  TiDBInfo getDatabase(long id) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(id);
    ByteString json = MetaCodec.hashGet(KEY_DBs, dbKey, this.snapshot);
    if (json == null || json.isEmpty()) {
      return null;
    }
    return parseFromJson(json, TiDBInfo.class);
  }

  List<TiTableInfo> getTables(long dbId) {
    ByteString dbKey = MetaCodec.encodeDatabaseID(dbId);
    List<Pair<ByteString, ByteString>> fields = MetaCodec.hashGetFields(dbKey, this.snapshot);
    ImmutableList.Builder<TiTableInfo> builder = ImmutableList.builder();
    for (Pair<ByteString, ByteString> pair : fields) {
      if (KeyUtils.hasPrefix(pair.first, ByteString.copyFromUtf8(MetaCodec.KEY_TABLE))) {
        try {
          TiTableInfo tableInfo = parseFromJson(pair.second, TiTableInfo.class);
          if (!tableInfo.isSequence()) {
            builder.add(tableInfo);
          }
        } catch (TiClientInternalException e) {
          logger.warn("fail to parse table from json!", e);
        }
      }
    }
    return builder.build();
  }
}
