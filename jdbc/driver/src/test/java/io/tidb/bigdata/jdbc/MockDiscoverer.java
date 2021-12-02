/*
 * Copyright 2021 TiKV Project Authors.
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

package io.tidb.bigdata.jdbc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class MockDiscoverer implements Discoverer {

  private String[] result;

  MockDiscoverer(String[] result) {
    this.result = result;
  }

  void setResult(String[] result) {
    this.result = result;
  }

  @Override
  public String[] getAndReload() {
    return result;
  }

  public String[] get() {
    return getAndReload();
  }

  @Override
  public Future<String[]> reload() {
    return CompletableFuture.completedFuture(result);
  }

  @Override
  public void succeeded(String backend) {
  }

  @Override
  public void failed(String backend) {
  }

  @Override
  public long getLastReloadTime() {
    return 0;
  }
}
