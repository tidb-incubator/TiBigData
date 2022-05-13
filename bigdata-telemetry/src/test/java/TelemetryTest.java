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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import io.tidb.bigdata.telemetry.TeleMsg;
import io.tidb.bigdata.telemetry.Telemetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TelemetryTest {
    HttpServer server;
    @Before
    public void startServer() throws IOException {
        HttpServerProvider provider = HttpServerProvider.provider();
        server = provider.createHttpServer(new InetSocketAddress(8989), 0);
        server.createContext("/test", new TestHandler());
        server.start();
    }
    @After
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void testTelemetry() throws JsonProcessingException {
        Telemetry telemetry = new Telemetry();
        TeleMsg teleMsg = new TeleMsg() {
            @Override
            public String setTrackId() {
                return "testId";
            }

            @Override
            public String setSubName() {
                return "telemetryTest";
            }

            @Override
            public Map<String, Object> setInstance() {
                Map<String, Object> instance = new HashMap<String, Object>();
                instance.put("version", "test01");
                return instance;
            }

            @Override
            public Map<String, Object> setContent() {
                Map<String, Object> content = new HashMap<String, Object>();
                content.put("cost_time", "1000");
                return null;
            }
        };
        ObjectMapper mapper = new ObjectMapper();
        String msgString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(teleMsg);
        System.out.println(msgString);
        telemetry.setUrl("http://127.0.0.1:8989/test");
        assert (Telemetry.ReportState.SUCCESS.equals(telemetry.report(teleMsg)));
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
}