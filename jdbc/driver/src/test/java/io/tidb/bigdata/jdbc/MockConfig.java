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

public class MockConfig {

  public String bootstrapUrl;
  public String[] backends;
  public String[] ip;
  public String backend;
  public int[] port;
  private boolean blocked;

  public MockConfig() {}

  public MockConfig(String[] backends, String[] ip, int[] port) {
    this.backends = backends;
    this.ip = ip;
    this.port = port;
  }

  public synchronized void block() {
    blocked = true;
  }

  public synchronized void unblock() {
    blocked = false;
    notifyAll();
  }

  public synchronized void checkBlocked() {
    while (blocked) {
      try {
        wait();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        return;
      }
    }
  }
}
