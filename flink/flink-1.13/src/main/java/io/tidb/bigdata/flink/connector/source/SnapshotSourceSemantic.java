/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.source;

import java.util.Arrays;

public enum SnapshotSourceSemantic {
  AT_LEAST_ONCE("at-least-once"),
  EXACTLY_ONCE("exactly-once");

  private final String semantic;

  SnapshotSourceSemantic(String semantic) {
    this.semantic = semantic;
  }

  public String getSemantic() {
    return semantic;
  }

  public static SnapshotSourceSemantic fromString(String s) {
    return Arrays.stream(values())
        .filter(value -> value.getSemantic().equalsIgnoreCase(s))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Unsupported snapshot source semantic: " + s));
  }
}
