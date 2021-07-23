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

package io.tidb.bigdata.trino.tidb;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import java.util.Map;

public final class TiDBConnectorFactory implements ConnectorFactory {

  @Override
  public String getName() {
    return "tidb";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new TiDBHandleResolver();
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(config, "config is null");

    try {
      Bootstrap app = new Bootstrap(
          new JsonModule(),
          new TiDBModule(catalogName, context.getTypeManager()));

      Injector injector = app
          .strictConfig()
          .doNotInitializeLogging()
          .setRequiredConfigurationProperties(config)
          .initialize();

      return injector.getInstance(TiDBConnector.class);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
