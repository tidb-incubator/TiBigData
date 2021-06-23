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

package io.tidb.bigdata.flink.tidb;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public enum StreamingReadableMetadata {
  COMMIT_TIMESTAMP("commit_timestamp",
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()),
  COMMIT_VERSION("commit_version", DataTypes.BIGINT().notNull());

  final String key;
  final DataType type;

  StreamingReadableMetadata(final String key, final DataType type) {
    this.key = key;
    this.type = type;
  }
}