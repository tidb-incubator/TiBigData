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

package io.tidb.bigdata.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class HttpClientUtil {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * HTTP POST
     *
     * @param url    server url
     * @param msg    post entry object
     * @return HttpResponse response
     */
    public HttpResponse postJSON(String url, Object msg) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String msgString = mapper.writeValueAsString(msg);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.setEntity(new StringEntity(msgString));
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse resp = httpClient.execute(httpPost);
            checkResp(url, resp);
            return resp;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * HTTP GET
     *
     * @param url   server url
     * @return HttpResponse response
     */
    public HttpResponse get(String url) throws IOException {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(url);
            httpGet.setHeader("Content-Type", "application/json");
            HttpResponse resp = httpClient.execute(httpGet);
            checkResp(url, resp);
            return resp;
        } catch (Exception e) {
                throw e;
        }
    }

    private void checkResp(String url, HttpResponse resp) {
        if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            logger.info(
                    String.format("Failed to get HTTP request: %s, response: %s, code: %d.",
                            url, resp.getEntity().toString(), resp.getStatusLine().getStatusCode()));
        }
    }
}