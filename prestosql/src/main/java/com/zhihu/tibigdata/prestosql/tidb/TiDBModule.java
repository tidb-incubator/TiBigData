/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.prestosql.tidb;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.type.TypeManager;

public final class TiDBModule
    implements Module {

  private final String connectorId;
  private final TypeManager typeManager;

  public TiDBModule(String connectorId, TypeManager typeManager) {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(TiDBConnector.class).in(Scopes.SINGLETON);
    binder.bind(TiDBConnectorId.class).toInstance(new TiDBConnectorId(connectorId));
    binder.bind(TiDBMetadata.class).in(Scopes.SINGLETON);
    binder.bind(TiDBSession.class).in(Scopes.SINGLETON);
    binder.bind(TiDBSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(TiDBRecordSetProvider.class).in(Scopes.SINGLETON);

    configBinder(binder).bindConfig(TiDBConfig.class);
  }
}
