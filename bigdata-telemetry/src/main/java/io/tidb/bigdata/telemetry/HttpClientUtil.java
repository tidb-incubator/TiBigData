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
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtil {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  /**
   * HTTP POST. Please close the CloseableHttpResponse in a final clause. {@link
   * org.apache.http.client.methods.CloseableHttpResponse}
   *
   * @param url server url
   * @param msg post entry object
   * @return CloseableHttpResponse
   */
  public CloseableHttpResponse postJSON(String url, Object msg) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String msgString = mapper.writeValueAsString(msg);
    HttpPost httpPost = new HttpPost(url);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      httpPost.setEntity(new StringEntity(msgString));
      httpPost.setHeader("Content-Type", "application/json");
      CloseableHttpResponse resp = httpClient.execute(httpPost);
      resp.close();
      if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn(
            String.format(
                "Failed to get HTTP request: %s, response: %s, code: %d.",
                url, resp.getEntity().toString(), resp.getStatusLine().getStatusCode()));
      }
      return resp;
    } finally {
      httpPost.releaseConnection();
    }
  }

  /**
   * HTTP GET. Please close the CloseableHttpResponse in a final clause. {@link
   * org.apache.http.client.methods.CloseableHttpResponse}
   *
   * @param url server url
   * @return CloseableHttpResponse
   */
  public CloseableHttpResponse get(String url) throws Exception {
    HttpGet httpGet = new HttpGet(url);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      CloseableHttpResponse resp = httpClient.execute(httpGet);
      if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn(
            String.format(
                "Failed to get HTTP request: %s, response: %s, code: %d.",
                url, resp.getEntity().toString(), resp.getStatusLine().getStatusCode()));
      }
      return resp;
    } finally {
      httpGet.releaseConnection();
    }
  }
}
