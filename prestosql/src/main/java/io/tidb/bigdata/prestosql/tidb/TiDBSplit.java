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

package io.tidb.bigdata.prestosql.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.tidb.bigdata.tidb.SplitInternal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class TiDBSplit
    implements ConnectorSplit {

  private String pdAddresses;
  private TiDBTableHandle table;
  private String startKey;
  private String endKey;
  private Optional<String> additionalPredicate;

  @JsonCreator
  public TiDBSplit(
      @JsonProperty("table") TiDBTableHandle table,
      @JsonProperty("startKey") String startKey,
      @JsonProperty("endKey") String endKey,
      @JsonProperty("additionalPredicate") Optional<String> additionalPredicate) {
    this.table = requireNonNull(table, "table is null");
    this.startKey = requireNonNull(startKey, "startKey is null");
    this.endKey = requireNonNull(endKey, "endKey is null");
    this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
  }

  TiDBSplit(SplitInternal from, Optional<String> additionalPredicate) {
    this(new TiDBTableHandle(from.getTable()), from.getStartKey(), from.getEndKey(),
        additionalPredicate);
  }

  @Override
  public Object getInfo() {
    return this;
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses() {
    return ImmutableList.of();
  }

  @JsonProperty
  public TiDBTableHandle getTable() {
    return table;
  }

  @JsonProperty
  public String getStartKey() {
    return startKey;
  }

  @JsonProperty
  public String getEndKey() {
    return endKey;
  }

  @JsonProperty
  public Optional<String> getAdditionalPredicate() {
    return additionalPredicate;
  }

  SplitInternal toInternal() {
    return new SplitInternal(getTable().getInternal(), getStartKey(), getEndKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(pdAddresses, table, startKey, endKey);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TiDBSplit other = (TiDBSplit) obj;
    return Objects.equals(this.pdAddresses, other.pdAddresses)
        && Objects.equals(this.table, other.table)
        && Objects.equals(this.startKey, other.startKey)
        && Objects.equals(this.endKey, other.endKey);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pdAddresses", pdAddresses)
        .add("table", table)
        .add("startKey", startKey)
        .add("endKey", endKey)
        .toString();
  }
}
