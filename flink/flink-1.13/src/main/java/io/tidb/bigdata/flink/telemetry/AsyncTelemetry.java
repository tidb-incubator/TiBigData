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

package io.tidb.bigdata.flink.telemetry;

import io.tidb.bigdata.telemetry.Telemetry;
import io.tidb.bigdata.telemetry.Telemetry.ReportState;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTelemetry {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTelemetry.class);

  private Map<String, String> properties;

  public AsyncTelemetry(Map<String, String> properties) {
    this.properties = properties;
  }

  public void report() {
    try {
      if (FlinkTeleMsg.getInstance().shouldSendMsg())
        CompletableFuture.runAsync(task);
    } catch (NullPointerException e) {
      CompletableFuture.runAsync(task);
    } catch (Exception e) {
      LOG.warn("Failed to report telemetry. " + e.getMessage());
    }
  }

  public Runnable task =
      () -> {
        try {
          Telemetry telemetry = new Telemetry();
          FlinkTeleMsg teleMsg = FlinkTeleMsg.getInstance(properties);
          synchronized (AsyncTelemetry.class) {
            if (teleMsg.shouldSendMsg()) {
              teleMsg.changeState(FlinkTeleMsg.FlinkTeleMsgState.SENT);
              ReportState reportState = telemetry.report(teleMsg);
              LOG.info("Telemetry state: " + reportState);
            }
          }
        } catch (Exception e) {
          LOG.info("Failed to build flink-1.13 telemetry message. " + e.getMessage());
        }
      };
}
