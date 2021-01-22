/*
 * Copyright 2020 org.tikv.
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

package org.tikv.bigdata.prestodb.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;

public final class TiDBTableLayoutHandle implements ConnectorTableLayoutHandle {

  private final TiDBTableHandle table;
  private final Optional<TupleDomain<ColumnHandle>> tupleDomain;
  private final Optional<String> additionalPredicate;

  @JsonCreator
  public TiDBTableLayoutHandle(
      @JsonProperty("table") TiDBTableHandle table,
      @JsonProperty("tupleDomain") Optional<TupleDomain<ColumnHandle>> tupleDomain,
      @JsonProperty("additionalPredicate") Optional<String> additionalPredicate) {
    this.table = table;
    this.tupleDomain = tupleDomain;
    this.additionalPredicate = additionalPredicate;
  }

  @JsonProperty
  public TiDBTableHandle getTable() {
    return table;
  }

  @JsonProperty
  public Optional<TupleDomain<ColumnHandle>> getTupleDomain() {
    return tupleDomain;
  }

  @JsonProperty
  public Optional<String> getAdditionalPredicate() {
    return additionalPredicate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TiDBTableLayoutHandle that = (TiDBTableLayoutHandle) o;
    return Objects.equals(table, that.table)
        && Objects.equals(tupleDomain, that.tupleDomain)
        && Objects.equals(additionalPredicate, that.additionalPredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, tupleDomain, additionalPredicate);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("tupleDomain", tupleDomain)
        .add("additionalPredicate", additionalPredicate)
        .toString();
  }
}
