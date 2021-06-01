/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.mapreduce.tidb;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

public class TiDBConfiguration {
  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "mapreduce.jdbc.url";

  public static final String DATABASE_NAME = "mapreduce.jdbc.database";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "mapreduce.jdbc.username";

  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "mapreduce.jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = "mapreduce.jdbc.input.table.name";

  /** scan record limit per mapper */
  public static final String MAPPER_RECORD_LIMIT = "mapreduce.mapper.record.limit";

  /** tidb snapshot */
  public static final String SNAPSHOT = "mapreduce.tidb.snapshot";

  /** timestamp format */
  public static final String TIMESTAMP_FORMAT_PREFIX = "timestamp-format";

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY =
      "mapreduce.jdbc.input.field.names";

  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY =
      "mapreduce.jdbc.input.class";


  /**
   * Sets the TiDB access related fields in the {@link Configuration}.
   * @param conf the configuration
   * @param dbUrl JDBC DB access URL.
   * @param userName DB access username
   * @param password DB access passwd
   */
  public static void configureDB(Configuration conf,
      String dbUrl, String databaseName, String userName, String password) {

    conf.set(URL_PROPERTY, dbUrl);
    conf.set(DATABASE_NAME, databaseName);
    conf.set(USERNAME_PROPERTY, userName);
    conf.set(PASSWORD_PROPERTY, password);
  }

  private Configuration conf;

  public TiDBConfiguration(Configuration conf) {
    this.conf = conf;
  }

  public ClientSession getSingleConnection() {

    Map<String, String> properties = new HashMap<>(3);
    properties.put(ClientConfig.DATABASE_URL, conf.get(URL_PROPERTY));
    properties.put(ClientConfig.USERNAME, conf.get(USERNAME_PROPERTY));
    properties.put(ClientConfig.PASSWORD, conf.get(PASSWORD_PROPERTY));

    return ClientSession.createWithSingleConnection(new ClientConfig(properties));
  }

  public Connection getJdbcConnection() throws SQLException {
    return DriverManager.getConnection(conf.get(URL_PROPERTY), conf.get(USERNAME_PROPERTY),
        conf.get(PASSWORD_PROPERTY));
  }

  public Configuration getConf() {
    return conf;
  }

  public void setInputTableName(String tableName) {
    conf.set(TiDBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
  }

  public String[] getInputFieldNames() {
    return conf.getStrings(TiDBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
  }

  public void setInputFieldNames(String[] fieldNames) {
    conf.setStrings(TiDBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  public void setMapperRecordLimit(Integer limit) {
    conf.setInt(TiDBConfiguration.MAPPER_RECORD_LIMIT, limit);
  }

  public void setSnapshot(String snapshot) {
    conf.set(TiDBConfiguration.SNAPSHOT, snapshot);
  }


  public Class<?> getInputClass() {
    return conf.getClass(TiDBConfiguration.INPUT_CLASS_PROPERTY,
        NullDBWritable.class);
  }

  public void setInputClass(Class<? extends TiDBWritable> inputClass) {
    conf.setClass(TiDBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
        TiDBWritable.class);
  }

  public String getInputTableName() {
    return conf.get(INPUT_TABLE_NAME_PROPERTY);
  }

  public String getDatabaseName() {
    return conf.get(DATABASE_NAME);
  }

  public Integer getMapperRecordLimit() {
    return conf.getInt(TiDBConfiguration.MAPPER_RECORD_LIMIT, Integer.MAX_VALUE);
  }

  public String getSnapshot() {
    return conf.get(TiDBConfiguration.SNAPSHOT);
  }

}

