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

package io.tidb.bigdata.flink.tidb.telemetry;

import static java.lang.String.format;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import io.tidb.bigdata.flink.telemetry.FlinkTeleMsg;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.TableUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TelemetryTest extends FlinkTestBase {
  HttpServer server;

  @Before
  public void startServer() throws IOException {
    HttpServerProvider provider = HttpServerProvider.provider();
    server = provider.createHttpServer(new InetSocketAddress(8989), 0);
    server.createContext("/test", new TestHandler());
    server.start();
  }

  public class TestHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      String response = "test telemetry";
      httpExchange.sendResponseHeaders(200, 0);
      OutputStream os = httpExchange.getResponseBody();
      os.write(response.getBytes(StandardCharsets.UTF_8));
      os.close();
    }
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  @Test
  public void testTelemetry() throws InterruptedException {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put("type", "tidb");
    properties.put("tidb.telemetry.enable", "false");
    properties.put("tidb.telemetry.url", "http://127.0.0.1:8989/test");
    TableEnvironment tableEnvironment = getBatchTableEnvironment();
    Assert.assertThrows(NullPointerException.class, FlinkTeleMsg::validateAndGet);
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String showDatabases = String.format("SHOW DATABASES");
    tableEnvironment.executeSql(showDatabases);
    Thread.sleep(1000);
    Assert.assertThrows(NullPointerException.class, FlinkTeleMsg::validateAndGet);
    properties.put("tidb.telemetry.enable", "true");
    createCatalogSql =
        format("CREATE CATALOG `tidb2` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    tableEnvironment.executeSql(showDatabases);
    Thread.sleep(1000);
    Assert.assertEquals(false, FlinkTeleMsg.validateAndGet().shouldSendMsg());
  }
}
