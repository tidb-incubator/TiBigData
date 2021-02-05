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

package io.tidb.bigdata.prestodb.tidb;

import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.Wrapper;
import javax.inject.Inject;

public final class TiDBSession extends Wrapper<ClientSession> {

  @Inject
  public TiDBSession(TiDBConfig config) {
    super(ClientSession.create(config.getInternal()));
  }
}
