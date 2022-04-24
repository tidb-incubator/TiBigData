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

package io.tidb.bigdata.hive;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class TiDBStorageHandler implements HiveStorageHandler {

  private Configuration configuration;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return TiDBHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return new OutputFormat<Object, Object>() {

      @Override
      public RecordWriter getRecordWriter(
          FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
          throws IOException {
        throw new UnsupportedOperationException("Writing to TiDB is unsupported now");
      }

      @Override
      public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException("Writing to TiDB is unsupported now");
      }
    }.getClass();
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return TiDBSerde.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {}

  @Override
  public void configureOutputJobProperties(
      TableDesc tableDesc, Map<String, String> jobProperties) {}

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {}

  /**
   * Put all tidb properties into jobConf. JobConf contains all properties in hive session and
   * session properties should override table properties except for immutable properties such as
   * url, username, password(see {@link TiDBConstant#IMMUTABLE_CONFIG}).
   *
   * <p>TODO: Hide password in jobConf
   */
  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    Maps.fromProperties(tableDesc.getProperties())
        .forEach(
            (key, value) -> {
              // If property is immutable in session or jobConf is empty,
              // we use table property to overwrite it;
              if (TiDBConstant.IMMUTABLE_CONFIG.contains(key)
                  || StringUtils.isEmpty(jobConf.get(key))) {
                jobConf.set(key, value);
              }
            });
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }
}
