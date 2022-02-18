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

package io.tidb.bigdata.tidb;

import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider;
import io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider;
import io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.JdbcConnectionProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class JdbcConnectionProviderFactoryTest {

  @Test
  public void testCreateConnectionProvider() throws Exception {
    ClientConfig clientConfig = new ClientConfig(ConfigUtils.defaultProperties());
    clientConfig.setJdbcConnectionProviderImpl(BasicJdbcConnectionProvider.class.getName());
    JdbcConnectionProvider provider = JdbcConnectionProviderFactory.createJdbcConnectionProvider(
        clientConfig);
    Assert.assertTrue(provider instanceof BasicJdbcConnectionProvider);
    provider.close();

    clientConfig.setJdbcConnectionProviderImpl(
        HikariDataSourceJdbcConnectionProvider.class.getName());
    provider = JdbcConnectionProviderFactory.createJdbcConnectionProvider(
        clientConfig);
    provider.close();
    Assert.assertTrue(provider instanceof HikariDataSourceJdbcConnectionProvider);
  }

}