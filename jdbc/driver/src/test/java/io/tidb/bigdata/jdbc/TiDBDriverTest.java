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

import io.tidb.bigdata.jdbc.impl.DiscovererImpl;
import io.tidb.bigdata.jdbc.impl.RandomShuffleUrlMapper;
import io.tidb.bigdata.jdbc.impl.RoundRobinUrlMapper;
import io.tidb.bigdata.test.IntegrationTest;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TiDBDriverTest {

  public static final String JDBC_DRIVER = "io.tidb.bigdata.jdbc.TiDBDriver";

  public static final String TIDB_URL = "jdbc:tidb://127.0.0.1:4000?user=root&password=";
  public static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:4000?user=root&password=";
  public static final String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orcl";

  public static final String[] backends1 =
      new String[] {
        "jdbc:mysql://127.0.0.1:4000?user=root&password=",
        "jdbc:mysql://127.0.0.1:4001?user=root&password=",
        "jdbc:mysql://127.0.0.1:4002?user=root&password=",
        "jdbc:mysql://127.0.0.1:4003?user=root&password=",
        "jdbc:mysql://127.0.0.1:4004?user=root&password=",
        "jdbc:mysql://127.0.0.1:4005?user=root&password=",
        "jdbc:mysql://127.0.0.1:4006?user=root&password=",
        "jdbc:mysql://127.0.0.1:4007?user=root&password=",
        "jdbc:mysql://127.0.0.1:4008?user=root&password=",
      };

  public static final String[] backends2 =
      new String[] {
        "jdbc:mysql://127.0.0.1:4004?user=root&password=",
        "jdbc:mysql://127.0.0.1:4005?user=root&password=",
        "jdbc:mysql://127.0.0.1:4006?user=root&password=",
      };

  public static final String[] backends3 =
      new String[] {
        "jdbc:mysql://127.0.0.1:4007?user=root&password=",
        "jdbc:mysql://127.0.0.2:4008?user=root&password=",
        "jdbc:mysql://127.0.0.3:4009?user=root&password=",
      };

  public static final String[] ip =
      new String[] {
        "127.0.0.1", "127.0.0.2", "127.0.0.3",
      };

  public static final int[] port =
      new int[] {
        4007, 4008, 4009,
      };

  @Test
  public void testTiDBDriver() throws ClassNotFoundException, SQLException {
    Class.forName(JDBC_DRIVER);
    Assert.assertNotNull(DriverManager.getDriver(TIDB_URL));
  }

  @Test
  public void testLoadBalancingDriver() throws SQLException {
    final MockDriver driver = new MockDriver();
    final MockUrlMapper mapper = new MockUrlMapper(backends2, backends1);
    final LoadBalancingDriver loadBalancingDriver =
        new LoadBalancingDriver(
            "jdbc:tidb://", mapper, driver, (d, b, i, e) -> new MockDiscoverer(backends1));
    Assert.assertTrue(loadBalancingDriver.acceptsURL(TIDB_URL));
    Assert.assertTrue(loadBalancingDriver.acceptsURL(MYSQL_URL));
    Assert.assertFalse(loadBalancingDriver.acceptsURL(ORACLE_URL));
    final Properties prop1 = new Properties();
    prop1.setProperty("k1", "v1");
    final Properties prop2 = (Properties) prop1.clone();
    prop2.setProperty("k2", "v2");
    final MockConnection conn0 = (MockConnection) loadBalancingDriver.connect(TIDB_URL, null);
    Assert.assertNotNull(conn0);
    final MockConnection conn1 = (MockConnection) loadBalancingDriver.connect(TIDB_URL, prop1);
    Assert.assertNotNull(conn1);
    final MockConnection conn2 = (MockConnection) loadBalancingDriver.connect(TIDB_URL, prop2);
    Assert.assertNotNull(conn2);
    Assert.assertNotEquals(conn0, conn1);
    Assert.assertNotEquals(conn1, conn2);
    Assert.assertEquals(conn0.getConfig().backend, backends2[0]);
    Assert.assertEquals(conn1.getConfig().backend, backends2[0]);
    Assert.assertEquals(conn2.getConfig().backend, backends2[0]);
    mapper.setResult(backends3);
    Assert.assertEquals(
        ((MockConnection) ((MockConnection) loadBalancingDriver.connect(TIDB_URL, prop2)))
            .getConfig()
            .backend,
        backends3[0]);
  }

  @Test
  public void testRandomShuffleUrlMapper() {
    final RandomShuffleUrlMapper mapper = new RandomShuffleUrlMapper();
    final Set<String> allBackends = Arrays.stream(backends1).collect(Collectors.toSet());
    for (final String backend : mapper.apply(backends1)) {
      Assert.assertTrue(allBackends.contains(backend));
      allBackends.remove(backend);
    }
    Assert.assertTrue(allBackends.isEmpty());
    while (Arrays.equals(backends1, mapper.apply(backends1))) {}
  }

  @Test
  public void testUrlMapperFactory() {
    Assert.assertTrue(
        LoadBalancingDriver.createUrlMapper("random") instanceof RandomShuffleUrlMapper);
    Assert.assertTrue(
        LoadBalancingDriver.createUrlMapper("roundrobin") instanceof RoundRobinUrlMapper);
    try {
      LoadBalancingDriver.createUrlMapper("unknown");
      Assert.fail();
    } catch (final Exception ignored) {
    }
  }

  @Test
  public void testRoundRobinUrlMapper() {
    final RoundRobinUrlMapper mapper = new RoundRobinUrlMapper();
    for (int idx = 0; idx < 10; idx++) {
      Assert.assertArrayEquals(backends1, mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
          },
          mapper.apply(backends1));
      Assert.assertArrayEquals(
          new String[] {
            "jdbc:mysql://127.0.0.1:4008?user=root&password=",
            "jdbc:mysql://127.0.0.1:4000?user=root&password=",
            "jdbc:mysql://127.0.0.1:4001?user=root&password=",
            "jdbc:mysql://127.0.0.1:4002?user=root&password=",
            "jdbc:mysql://127.0.0.1:4003?user=root&password=",
            "jdbc:mysql://127.0.0.1:4004?user=root&password=",
            "jdbc:mysql://127.0.0.1:4005?user=root&password=",
            "jdbc:mysql://127.0.0.1:4006?user=root&password=",
            "jdbc:mysql://127.0.0.1:4007?user=root&password=",
          },
          mapper.apply(backends1));
    }
  }

  @Test
  public void testDiscoverer() throws Exception {
    final MockConfig config = new MockConfig(backends3, ip, port);
    final MockDriver driver = new MockDriver(config);
    final DiscovererImpl discoverer =
        new DiscovererImpl(driver, MYSQL_URL, null, ForkJoinPool.commonPool());
    String[] backends = discoverer.reload().get();
    Assert.assertArrayEquals(backends, backends3);
    config.block();
    discoverer.failed(backends3[0]);
    backends = discoverer.getAndReload();
    for (String backend : backends) {
      Assert.assertNotEquals(backend, backends3[0]);
    }
    Assert.assertEquals(backends.length, 2);
    discoverer.succeeded(backends3[0]);
    Assert.assertArrayEquals(discoverer.getAndReload(), backends3);
    config.unblock();
    Assert.assertArrayEquals(discoverer.reload().get(), backends3);
  }
}
