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

package io.tidb.bigdata.cdc;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/*
 * TiCDC row changed event value
 */
public abstract class RowChangedValue implements Value {

  private final RowColumn[] oldValue;
  private final RowColumn[] newValue;

  protected RowChangedValue(final RowColumn[] oldValue, final RowColumn[] newValue) {
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  public abstract Type getType();

  public RowColumn[] getOldValue() {
    return oldValue;
  }

  public RowColumn[] getNewValue() {
    return newValue;
  }

  public abstract Optional<RowDeletedValue> asDeleted();

  public abstract Optional<RowUpdatedValue> asUpdated();

  public abstract Optional<RowInsertedValue> asInserted();

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowChangedValue)) {
      return false;
    }

    final RowChangedValue other = (RowChangedValue) o;
    return Objects.equals(getType(), other.getType())
        && Arrays.deepEquals(getNewValue(), other.getNewValue())
        && Arrays.deepEquals(getOldValue(), other.getOldValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getNewValue(), getOldValue());
  }

  @Override
  public Optional<RowChangedValue> asRowChanged() {
    return Optional.of(this);
  }

  @Override
  public Optional<ResolvedValue> asResolved() {
    return Optional.empty();
  }

  @Override
  public Optional<DdlValue> asDdl() {
    return Optional.empty();
  }

  public enum Type {
    INSERT,
    UPDATE,
    DELETE
  }
}
