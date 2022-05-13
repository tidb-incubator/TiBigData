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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report telemetry by HTTP POST. The url should be constant
 */
public class Telemetry {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public String url = "http://127.0.0.1:2379";

    /**
     * Send telemetry message.
     *
     * @param msg the msg sent to telemetry server
     */
    public ReportState report(TeleMsg msg) {
        try {
            HttpClientUtil httpClient = new HttpClientUtil();
            ObjectMapper mapper = new ObjectMapper();
            String msgString = mapper.writeValueAsString(msg);
            logger.info("Telemetry report: " + msgString);
            HttpResponse resp = httpClient.postJSON(url, msg);
            return (resp.getStatusLine().getStatusCode() == HttpStatus.SC_OK)
                    ? ReportState.SUCCESS
                    : ReportState.FAILURE;
        } catch (Exception e) {
            logger.info("Failed to report telemetry. " + e.getMessage());
            return ReportState.FAILURE;
        }
    }

    public void setUrl(String url){
        this.url = url;
    }

    public enum ReportState {
        SUCCESS,
        FAILURE;
    }
}