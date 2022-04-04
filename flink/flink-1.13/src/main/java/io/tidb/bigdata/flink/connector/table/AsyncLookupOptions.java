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

package io.tidb.bigdata.flink.connector.table;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions.Builder;


public class AsyncLookupOptions implements Serializable {

  private final boolean async;
  private final int maxPoolSize;
  private final long cacheMaxSize;
  private final long cacheExpireMs;


  private AsyncLookupOptions(boolean async, int maxPoolSize, long cacheMaxSize,
      long cacheExpireMs) {
    this.async = async;
    this.maxPoolSize = maxPoolSize;
    this.cacheMaxSize = cacheMaxSize;
    this.cacheExpireMs = cacheExpireMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AsyncLookupOptions that = (AsyncLookupOptions) o;
    return maxPoolSize == that.maxPoolSize && cacheMaxSize == that.cacheMaxSize
        && cacheExpireMs == that.cacheExpireMs;
  }

  public boolean isAsync() {
    return async;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public long getCacheMaxSize() {
    return cacheMaxSize;
  }

  public long getCacheExpireMs() {
    return cacheExpireMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxPoolSize, cacheMaxSize, cacheExpireMs);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private static final int DEFAULT_MAX_POOL_SIZE = 4;
    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
    private long cacheMaxSize = -1L;
    private long cacheExpireMs = -1L;
    private boolean async = false;

    public Builder setAsync(boolean async) {
      this.async = async;
      return this;
    }

    public Builder setMaxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public Builder setCacheMaxSize(long cacheMaxSize) {
      this.cacheMaxSize = cacheMaxSize;
      return this;
    }

    public Builder setCacheExpireMs(long cacheExpireMs) {
      this.cacheExpireMs = cacheExpireMs;
      return this;
    }

    public AsyncLookupOptions build() {
      return new AsyncLookupOptions(async, maxPoolSize, cacheMaxSize, cacheExpireMs);
    }
  }
}
