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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Telemetry message.
 */
public abstract class TeleMsg {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public String trackId;
    public String time;
    public String subName;
    public Map<String, Object> hardware;
    public Map<String, Object> instance;
    public Map<String, Object> content;

    protected TeleMsg(){
        this.time = setTime();
        this.hardware = generateHardwareInfo();
    }

    /**
     * set message track id
     * @return
     */
    public abstract String setTrackId();

    /**
     * application name
     * @return
     */
    public abstract String setSubName();

    /**
     * application version, TiDB version
     * @return
     */
    public abstract Map<String, Object> setInstance();

    /**
     * message content
     * @return
     */
    public abstract Map<String, Object> setContent();

    public String setTime() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    public Map<String, Object> generateHardwareInfo() {
        Map<String, Object> hardwareInfo = new HashMap<>();
        try {
            SystemInfoUtil systemInfoUtil = new SystemInfoUtil();
            hardwareInfo.put("os", systemInfoUtil.getOsFamily());
            hardwareInfo.put("version", systemInfoUtil.getOsVersion());
            hardwareInfo.put("cpu", systemInfoUtil.getCpu());
            hardwareInfo.put("memory", systemInfoUtil.getMemoryInfo());
            hardwareInfo.put("disks", systemInfoUtil.getDisks());
        } catch (Exception e) {
            logger.info("Failed to generate hardware information. " + e.getMessage());
        }
        return hardwareInfo;
    }
}