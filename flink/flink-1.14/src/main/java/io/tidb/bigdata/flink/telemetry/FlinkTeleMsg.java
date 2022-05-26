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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tidb.bigdata.telemetry.TeleMsg;
import io.tidb.bigdata.tidb.ClientConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FlinkTeleMsg is a single instance. Only send once in one Flink application. */
public class FlinkTeleMsg extends TeleMsg {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static volatile FlinkTeleMsg flinkTeleMsg;

  private static final String SUBNAME = "flink-1.14";
  private static final String TIBIGDATA_FLINK_VERSION = "0.0.5-SNAPSHOT";
  private volatile FlinkTeleMsgState sendState = FlinkTeleMsgState.UNSENT;
  private Map<String, String> properties;

  private FlinkTeleMsg(Map<String, String> properties) {
    this.properties = properties;
    this.instance = setInstance();
    this.trackId = setTrackId();
    this.content = setContent();
    this.subName = setSubName();
  }

  /**
   * Get FlinkTeleMsg single instance.
   *
   * @param properties tibigdata-flink properties.
   * @return The flinkTeleMsg is used to be reporting asynchronously.
   */
  public static FlinkTeleMsg getInstance(Map<String, String> properties) {
    if (flinkTeleMsg == null)
      synchronized (FlinkTeleMsg.class) {
        if (flinkTeleMsg == null) flinkTeleMsg = new FlinkTeleMsg(properties);
      }
    return flinkTeleMsg;
  }

  /**
   * * Get FlinkTeleMsg single instance. If flinkTeleMsg has been initialized, the object is
   * returned, otherwise an exception is thrown.
   *
   * @return flinkTeleMsg
   */
  public static FlinkTeleMsg getInstance() {
    return Preconditions.checkNotNull(flinkTeleMsg, "FlinkTeleMsg hasn't been initialized");
  }

  /**
   * Judge whether flinkTeleMsg has been sent.
   *
   * @return True is sent. False is not sent.
   */
  public boolean shouldSendMsg() {
    return sendState.equals(FlinkTeleMsgState.UNSENT);
  }

  public void changeState(FlinkTeleMsgState state) {
    this.sendState = state;
  }

  @Override
  public String setTrackId() {
    return JobID.generate().toString();
  }

  @Override
  public String setSubName() {
    return SUBNAME;
  }

  @Override
  public Map<String, Object> setInstance() {
    Map<String, Object> instance = new HashMap<>();
    instance.put("TiDBVersion", getTiDBVersion());
    instance.put("TiBigDataFlinkVersion", TIBIGDATA_FLINK_VERSION);
    instance.put("FlinkVersion", getFlinkVersion());
    return instance;
  }

  @Override
  public Map<String, Object> setContent() {
    Map<String, Object> content = new HashMap<>();
    Map<String, String> defaultConf = new HashMap<>();
    defaultConf.put("tidb.write_mode", "append");
    defaultConf.put("tidb.replica-read", "leader");
    defaultConf.put("sink.buffer-flush.max-rows", "100");
    defaultConf.put("sink.buffer-flush.interval", "1s");
    defaultConf.put("tidb.filter-push-down", "false");
    defaultConf.put("tidb.catalog.load-mode", "eager");
    defaultConf.put("tidb.sink.impl", "JDBC");
    defaultConf.put("tikv.sink.transaction", "MINIBATCH");
    defaultConf.put("tikv.sink.buffer-size", "1000");
    defaultConf.put("tikv.sink.row-id-allocator.step", "30000");
    defaultConf.put("tikv.sink.ignore-autoincrement-column-value", "false");
    defaultConf.put("tikv.sink.deduplicate", "false");
    for (Map.Entry<String, String> conf : defaultConf.entrySet()) {
      try {
        switch (conf.getKey()) {
          case "sink.buffer-flush.max-rows":
          case "tikv.sink.buffer-size":
          case "tikv.sink.row-id-allocator.step":
            content.put(
                conf.getKey(),
                Integer.parseInt(properties.getOrDefault(conf.getKey(), conf.getValue())));
            break;
          case "tidb.filter-push-down":
          case "tikv.sink.ignore-autoincrement-column-value":
          case "tikv.sink.deduplicate":
            content.put(
                conf.getKey(),
                Boolean.parseBoolean(properties.getOrDefault(conf.getKey(), conf.getValue())));
            break;
          default:
            content.put(conf.getKey(), properties.getOrDefault(conf.getKey(), conf.getValue()));
        }
      } catch (Exception e) {
        content.put(conf.getKey(), properties.getOrDefault(conf.getKey(), conf.getValue()));
      }
    }
    return content;
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    String msgString = "";
    try {
      msgString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.info("Failed to make telemetry message to string " + e.getMessage());
    }
    return msgString;
  }

  private String getTiDBVersion() {
    try {
      String pattern = "v[0-9]\\.[0-9]\\.[0-9]";
      Pattern r = Pattern.compile(pattern);
      String sql = "SELECT TIDB_VERSION()";
      String url = requireNonNull(properties.get(ClientConfig.DATABASE_URL));
      String username = requireNonNull(properties.get(ClientConfig.USERNAME));
      String password = requireNonNull(properties.get(ClientConfig.PASSWORD));
      Connection connection = DriverManager.getConnection(url, username, password);
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql);
      resultSet.next();
      Matcher m = r.matcher(resultSet.getString("tidb_version()"));
      if (m.find()) {
        return m.group(0);
      }
    } catch (Exception e) {
      logger.info("Failed to get TiDB version. " + e.getMessage());
    }
    return "UNKNOWN";
  }

  private String getFlinkVersion(){
    try{
      String flinkVersion = EnvironmentInformation.getVersion();
      return flinkVersion;
    } catch (Exception e) {
      return "UNKNOWN";
    }
  }

  public enum FlinkTeleMsgState {
    SENT,
    UNSENT
  }
}
