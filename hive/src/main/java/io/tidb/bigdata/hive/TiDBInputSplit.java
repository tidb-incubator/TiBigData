package io.tidb.bigdata.hive;

import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.mapred.InputSplit;
import org.tikv.common.meta.TiTimestamp;

public class TiDBInputSplit implements InputSplit {

  private String databaseName;
  private String tableName;
  private String startKey;
  private String endKey;
  private long physicalTimestamp;
  private long logicalTimestamp;


  public TiDBInputSplit() {
  }

  public TiDBInputSplit(SplitInternal splitInternal) {
    this(splitInternal.getTable().getSchemaName(),
        splitInternal.getTable().getTableName(),
        splitInternal.getStartKey(),
        splitInternal.getEndKey(),
        splitInternal.getTimestamp().getPhysical(),
        splitInternal.getTimestamp().getLogical());
  }

  public TiDBInputSplit(String databaseName, String tableName, String startKey,
      String endKey, long physicalTimestamp, long logicalTimestamp) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.startKey = startKey;
    this.endKey = endKey;
    this.physicalTimestamp = physicalTimestamp;
    this.logicalTimestamp = logicalTimestamp;
  }

  public SplitInternal toInternal() {
    return new SplitInternal(
        new TableHandleInternal(UUID.randomUUID().toString(), databaseName, tableName),
        startKey,
        endKey,
        new TiTimestamp(physicalTimestamp, logicalTimestamp));
  }


  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(databaseName);
    dataOutput.writeUTF(tableName);
    dataOutput.writeUTF(startKey);
    dataOutput.writeUTF(endKey);
    dataOutput.writeLong(physicalTimestamp);
    dataOutput.writeLong(logicalTimestamp);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.databaseName = dataInput.readUTF();
    this.tableName = dataInput.readUTF();
    this.startKey = dataInput.readUTF();
    this.endKey = dataInput.readUTF();
    this.physicalTimestamp = dataInput.readLong();
    this.logicalTimestamp = dataInput.readLong();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public long getPhysicalTimestamp() {
    return physicalTimestamp;
  }

  public long getLogicalTimestamp() {
    return logicalTimestamp;
  }
}
