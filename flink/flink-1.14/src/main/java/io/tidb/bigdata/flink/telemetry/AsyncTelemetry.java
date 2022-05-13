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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

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
        }
    }

    public Runnable task = () -> {
        Telemetry.ReportState reportState = Telemetry.ReportState.FAILURE;
        try {
            Telemetry telemetry = new Telemetry();
            FlinkTeleMsg teleMsg = FlinkTeleMsg.getInstance(properties);
            ReentrantLock lock = new ReentrantLock();
            lock.lock();
            try {
                if (teleMsg.shouldSendMsg()) {
                    reportState = telemetry.report(teleMsg);
                    teleMsg.changeState(FlinkTeleMsg.FlinkTeleMsgState.SENT);
                }
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            LOG.info("Failed to build flink-1.14 telemetry message. " + e.getMessage());
        } finally {
            LOG.info("Telemetry State: " + reportState);
        }
    };
}