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

import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_BUFFER_FLUSH_MAX_ROWS;
import static io.tidb.bigdata.flink.tidb.TiDBBaseCatalog.TIDB_CATALOG_LOAD_MODE;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_BUFFER_FLUSH_INTERVAL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tidb.bigdata.telemetry.TeleMsg;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.shade.com.google.protobuf.ByteString;

/** FlinkTeleMsg is a single instance. Only send once in one Flink application. */
public class FlinkTeleMsg extends TeleMsg {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static volatile FlinkTeleMsg flinkTeleMsg;

  private static final String SUBNAME = "flink-1.13";
  private static final String TIBIGDATA_FLINK_VERSION = "0.0.5-SNAPSHOT";
  private static final String TRACK_ID = "TiBigDataFlink1.13TelemetryID";
  private static final String TRACK_ID_PREFIX = "trkid_";
  private static final String APP_ID_PREFIX = "appid_";
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
   * Get FlinkTeleMsg single instance. If flinkTeleMsg has been initialized, the object is returned,
   * otherwise an exception is thrown.
   *
   * @return flinkTeleMsg
   */
  public static FlinkTeleMsg validateAndGet() {
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
    try (ClientSession clientSession = ClientSession.create(new ClientConfig(properties)); ) {
      ByteString value =
          clientSession.getTiSession().createSnapshot().get(ByteString.copyFromUtf8(TRACK_ID));

      if (!value.isEmpty()) {
        return value.toStringUtf8();
      }

      String uuid = TRACK_ID_PREFIX + UUID.randomUUID();
      TiDBWriteHelper tiDBWriteHelper =
          new TiDBWriteHelper(
              clientSession.getTiSession(), clientSession.getSnapshotVersion().getVersion());
      tiDBWriteHelper.preWriteFirst(new BytePairWrapper(TRACK_ID.getBytes(), uuid.getBytes()));
      tiDBWriteHelper.commitPrimaryKey();
      return uuid;
    } catch (Exception e) {
      logger.warn("Failed to generated telemetry track ID. " + e.getMessage());
      return APP_ID_PREFIX + JobID.generate();
    }
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
    defaultConf.put(TIDB_WRITE_MODE, "append");
    defaultConf.put(TIDB_REPLICA_READ, "leader");
    defaultConf.put(SINK_BUFFER_FLUSH_MAX_ROWS.key(), "100");
    defaultConf.put(SINK_BUFFER_FLUSH_INTERVAL.key(), "1s");
    defaultConf.put(TIDB_FILTER_PUSH_DOWN, "false");
    defaultConf.put(TIDB_CATALOG_LOAD_MODE, "eager");
    defaultConf.put("tidb.sink.impl", "JDBC");
    defaultConf.put("tikv.sink.transaction", "MINIBATCH");
    defaultConf.put("tikv.sink.buffer-size", "0");
    defaultConf.put("tikv.sink.row-id-allocator.step", "0");
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
      logger.warn("Failed to make telemetry message to string " + e.getMessage());
    }
    return msgString;
  }

  private String getTiDBVersion() {
    try (Connection connection =
            DriverManager.getConnection(
                properties.get(ClientConfig.DATABASE_URL),
                properties.get(ClientConfig.USERNAME),
                properties.get(ClientConfig.PASSWORD));
        Statement statement = connection.createStatement(); ) {
      String pattern = "v[0-9]\\.[0-9]\\.[0-9]";
      Pattern r = Pattern.compile(pattern);
      String sql = "SELECT TIDB_VERSION()";
      ResultSet resultSet = statement.executeQuery(sql);
      resultSet.next();
      Matcher m = r.matcher(resultSet.getString("tidb_version()"));
      if (m.find()) {
        return m.group(0);
      }
    } catch (Exception e) {
      logger.warn("Failed to get TiDB version. " + e.getMessage());
    }
    return "UNKNOWN";
  }

  private String getFlinkVersion() {
    try {
      String flinkVersion = EnvironmentInformation.getVersion();
      return flinkVersion;
    } catch (Exception e) {
      logger.warn("Failed to get Flink version. " + e.getMessage());
      return "UNKNOWN";
    }
  }

  public enum FlinkTeleMsgState {
    SENT,
    UNSENT
  }
}
