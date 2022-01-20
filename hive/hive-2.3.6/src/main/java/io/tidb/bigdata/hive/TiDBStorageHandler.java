package io.tidb.bigdata.hive;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
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
      public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
          Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("Writing to TiDB is unsupported now");
      }

      @Override
      public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        throw new UnsupportedOperationException("Writing to TiDB is unsupported now");
      }
    }.getClass();
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
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
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

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
