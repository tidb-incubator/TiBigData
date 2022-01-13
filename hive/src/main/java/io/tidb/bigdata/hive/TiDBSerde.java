package io.tidb.bigdata.hive;

import static io.tidb.bigdata.hive.TiDBConstant.DATABASE_NAME;
import static io.tidb.bigdata.hive.TiDBConstant.TABLE_NAME;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TiDBSerde extends AbstractSerDe {

  private String databaseName;
  private String tableName;
  private List<ColumnHandleInternal> columns;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties properties)
      throws SerDeException {
    this.tableName = Objects.requireNonNull(properties.getProperty(TABLE_NAME),
        TABLE_NAME + " can not be null");
    this.databaseName = Objects.requireNonNull(properties.getProperty(DATABASE_NAME),
        DATABASE_NAME + " can not be null");
    Map<String, String> map = new HashMap<>((Map) properties);
    try (ClientSession clientSession = ClientSession.create(
        new ClientConfig(map))) {
      columns = clientSession.getTableColumns(databaseName,
          tableName).orElseThrow(() -> new IllegalStateException("Can not get columns"));
    } catch (Exception e) {
      throw new SerDeException(e);
    }

  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    Object[] objects = (Object[]) o;
    MapWritable mapWritable = new MapWritable();
    for (int i = 0; i < columns.size(); i++) {
      ColumnHandleInternal column = columns.get(i);
      String name = column.getName();
      Object value = objects[i];
      mapWritable.put(new Text(name), TypeUtils.toWriteable(value, column.getType()));
    }
    return mapWritable;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    MapWritable mapWritable = (MapWritable) writable;
    Object[] objects = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      ColumnHandleInternal column = columns.get(i);
      Writable value = mapWritable.get(new Text(column.getName()));
      objects[i] = value;
    }
    return objects;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    List<ObjectInspector> list = new ArrayList<>();
    columns.forEach(column -> list.add(TypeUtils.toObjectInspector(column.getType())));
    List<String> columnNames = columns.stream().map(ColumnHandleInternal::getName)
        .collect(Collectors.toList());
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, list);
  }
}
