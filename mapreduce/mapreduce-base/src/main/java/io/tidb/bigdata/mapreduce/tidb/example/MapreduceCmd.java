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

package io.tidb.bigdata.mapreduce.tidb.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;

public class MapreduceCmd {
  @Parameter(
      names = {"-field", "-f"},
      description = "fields need query")
  public List<String> fields = new ArrayList<>();

  @Parameter(
      names = {"-databaseurl", "-du"},
      description = "database url",
      required = true)
  public String databaseUrl;

  @Parameter(
      names = {"-username", "-u"},
      description = "username",
      required = true)
  public String username;

  @Parameter(
      names = {"-password", "-p"},
      description = "password",
      password = true,
      required = true)
  public String password;

  @Parameter(
      names = {"-clusterTls"},
      description = "enable cluster TLS")
  public boolean clusterTlsEnabled = false;

  @Parameter(
      names = {"-clusterTlsCA"},
      description = "cluster TLS CA")
  public String clusterTlsCA;

  @Parameter(
      names = {"-clusterTlsCert"},
      description = "cluster TLS certificate")
  public String clusterTlsCert;

  @Parameter(
      names = {"-clusterTlsKey"},
      description = "cluster TLS key")
  public String clusterTlsKey;

  @Parameter(
      names = {"-clusteruseJks"},
      description = "use java key store")
  public boolean clusterUseJks = false;

  @Parameter(
      names = {"-clusterJksKeyPath"},
      description = "cluster Jks Key Path")
  public String clusterJksKeyPath;

  @Parameter(
      names = {"-clusterJksKeyPassword"},
      description = "cluster Jks Key password")
  public String clusterJksKeyPassword;

  @Parameter(
      names = {"-clusterJksTrustPath"},
      description = "cluster Jks Trust Path")
  public String clusterJksTrustPath;

  @Parameter(
      names = {"-clusterJksTrustPassword"},
      description = "cluster Jks Trust Password")
  public String clusterJksTrustPassword;

  @Parameter(
      names = {"-databasename", "-dn"},
      description = "database name",
      required = true)
  public String databaseName;

  @Parameter(
      names = {"-tablename", "-t"},
      description = "table name",
      required = true)
  public String tableName;

  @Parameter(
      names = {"-timestamp", "-ts"},
      description = "TiDB snapshot timestamp")
  public String timestamp;

  @Parameter(
      names = {"-limit", "-l"},
      description = "record limit per mapper")
  public Integer limit = Integer.MAX_VALUE;

  public MapreduceCmd(String[] args) {
    JCommander.newBuilder().addObject(this).build().parse(args);
  }
}
