/*
 * Copyright 2023 TiDB Project Authors.
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

package io.tidb.bigdata.flink.format.canal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlatMessage implements Serializable {

  private long id;
  private String database;
  private String table;
  private List<String> pkNames;
  private Boolean isDdl;
  private String type;
  // binlog executeTime
  private Long es;
  // dml build timeStamp
  private Long ts;
  private String sql;
  private Map<String, Integer> sqlType;
  private Map<String, String> mysqlType;
  private List<Map<String, String>> data;
  private List<Map<String, String>> old;

  @JsonProperty("_tidb")
  private TiDB tidb;

  public FlatMessage() {}

  public FlatMessage(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<String> getPkNames() {
    return pkNames;
  }

  public void addPkName(String pkName) {
    if (this.pkNames == null) {
      this.pkNames = new ArrayList<>();
    }
    this.pkNames.add(pkName);
  }

  public void setPkNames(List<String> pkNames) {
    this.pkNames = pkNames;
  }

  public Boolean getIsDdl() {
    return isDdl;
  }

  public void setIsDdl(Boolean isDdl) {
    this.isDdl = isDdl;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public Map<String, Integer> getSqlType() {
    return sqlType;
  }

  public void setSqlType(Map<String, Integer> sqlType) {
    this.sqlType = sqlType;
  }

  public Map<String, String> getMysqlType() {
    return mysqlType;
  }

  public void setMysqlType(Map<String, String> mysqlType) {
    this.mysqlType = mysqlType;
  }

  public List<Map<String, String>> getData() {
    return data;
  }

  public void setData(List<Map<String, String>> data) {
    this.data = data;
  }

  public List<Map<String, String>> getOld() {
    return old;
  }

  public void setOld(List<Map<String, String>> old) {
    this.old = old;
  }

  public Long getEs() {
    return es;
  }

  public void setEs(Long es) {
    this.es = es;
  }

  public Boolean getDdl() {
    return isDdl;
  }

  public void setDdl(Boolean ddl) {
    isDdl = ddl;
  }

  public TiDB getTidb() {
    return tidb;
  }

  public void setTidb(TiDB tidb) {
    this.tidb = tidb;
  }

  @Override
  public String toString() {
    return "FlatMessage{"
        + "id="
        + id
        + ", database='"
        + database
        + '\''
        + ", table='"
        + table
        + '\''
        + ", pkNames="
        + pkNames
        + ", isDdl="
        + isDdl
        + ", type='"
        + type
        + '\''
        + ", es="
        + es
        + ", ts="
        + ts
        + ", sql='"
        + sql
        + '\''
        + ", sqlType="
        + sqlType
        + ", mysqlType="
        + mysqlType
        + ", data="
        + data
        + ", old="
        + old
        + ", tidb="
        + tidb
        + '}';
  }

  public static class TiDB {

    private Long commitTs;
    private Long watermarkTs;

    public Long getCommitTs() {
      return commitTs;
    }

    public void setCommitTs(Long commitTs) {
      this.commitTs = commitTs;
    }

    public Long getWatermarkTs() {
      return watermarkTs;
    }

    public void setWatermarkTs(Long watermarkTs) {
      this.watermarkTs = watermarkTs;
    }

    @Override
    public String toString() {
      return "TiDB{" + "commitTs=" + commitTs + ", watermarkTs=" + watermarkTs + '}';
    }
  }
}
