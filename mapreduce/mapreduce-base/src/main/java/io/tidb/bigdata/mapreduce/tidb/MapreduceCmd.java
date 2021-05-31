/*
 * Copyright 2020 TiDB Project Authors.
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

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.PASSWORD;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.tidb.bigdata.tidb.ClientConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class MapreduceCmd {
  @Parameter(names = {"-field", "-f"}, description = "fields need query")
  public List<String> fields = new ArrayList<>();

  @Parameter(names = {"-databaseurl", "-du"}, description = "database url")
  public String databaseUrl;

  @Parameter(names = {"-username", "-u"}, description = "username")
  public String username;

  @Parameter(names = {"-password", "-p"}, description = "password", password = true)
  public String password;

  @Parameter(names = {"-databasename", "-dn"}, description = "database name")
  public String databaseName;

  @Parameter(names = {"-tablename", "-t"}, description = "table name")
  public String tableName;

  @Parameter(names = {"-timestamp", "-ts"}, description = "TiDB snapshot timestamp")
  public String timestamp;

  @Parameter(names = {"-limit", "-l"}, description = "record limit per mapper")
  public Integer limit = Integer.MAX_VALUE;

  public MapreduceCmd(String[] args) {
    JCommander.newBuilder().addObject(this).build().parse(args);
  }

  public Configuration toConf() {
    Configuration conf = new Configuration();

    String[] fileds = fields.toArray(new String[0]);
    conf.setStrings("tidb.field.names", fileds);
    conf.set(DATABASE_URL, databaseUrl);
    conf.set(USERNAME, username);
    conf.set(PASSWORD, password);
    conf.set("tidb.database.name", databaseName);
    conf.set("tidb.table.name", tableName);
    if (null != timestamp) {
      conf.set(ClientConfig.SNAPSHOT_TIMESTAMP, timestamp);
    }
    conf.setLong("mapper.record.limit", limit);

    return conf;
  }

}
