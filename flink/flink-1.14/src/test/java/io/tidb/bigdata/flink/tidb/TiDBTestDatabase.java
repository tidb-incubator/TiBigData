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

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.FlinkTestBase.DATABASE_NAME;

import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBTestDatabase extends ExternalResource {

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  private ClientSession clientSession;

  public ClientSession getClientSession() {
    return clientSession;
  }

  @Override
  protected void before() throws Throwable {
    this.clientSession = ClientSession.create(new ClientConfig(ConfigUtils.defaultProperties()));
    clientSession.sqlUpdate(String.format("CREATE DATABASE IF NOT EXISTS `%s`", DATABASE_NAME));
    logger.info("Create database {}", DATABASE_NAME);
  }

  @Override
  protected void after() {
    clientSession.sqlUpdate(String.format("DROP DATABASE IF EXISTS `%s`", DATABASE_NAME));
    logger.info("Drop database {}", DATABASE_NAME);
  }
}
