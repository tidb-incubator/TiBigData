package io.tidb.bigdata.hive;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.types.DataType;

public class TiDBRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBRecordReader.class);

  private final InputSplit split;
  private final Map<String, String> properties;

  private long pos;
  private ClientSession clientSession;
  private RecordCursorInternal cursor;
  private List<ColumnHandleInternal> columns;


  public TiDBRecordReader(InputSplit split, Map<String, String> properties) {
    this.split = split;
    this.properties = properties;
  }

  private void initClientSession() {
    if (clientSession != null) {
      return;
    }
    try {
      LOG.info("Init client session");
      clientSession = ClientSession.create(new ClientConfig(properties));
    } catch (Exception e) {
      throw new IllegalStateException("Can not init client session", e);
    }
  }

  private void initCursor() {
    if (cursor != null) {
      return;
    }
    TiDBInputSplit split = (TiDBInputSplit) this.split;
    columns = clientSession.getTableColumnsMust(split.getDatabaseName(), split.getTableName());
    RecordSetInternal recordSetInternal = new RecordSetInternal(
        clientSession,
        split.toInternal(),
        columns,
        Optional.empty(),
        Optional.of(split.toInternal().getTimestamp()));
    cursor = recordSetInternal.cursor();
  }

  @Override
  public boolean next(LongWritable longWritable, MapWritable mapWritable) throws IOException {
    initClientSession();
    initCursor();
    if (!cursor.advanceNextPosition()) {
      return false;
    }
    pos++;
    for (int i = 0; i < cursor.fieldCount(); i++) {
      ColumnHandleInternal column = columns.get(i);
      String name = column.getName();
      DataType type = column.getType();
      mapWritable.put(new Text(name), TypeUtils.toWriteable(cursor.getObject(i), type));
    }
    return true;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    try {
      cursor.close();
      clientSession.close();
    } catch (Exception e) {
      LOG.warn("Can not close session");
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
