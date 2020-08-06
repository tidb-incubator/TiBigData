package com.zhihu.flink.tidb.factory;

import com.google.common.collect.ImmutableSet;
import com.zhihu.flink.tidb.source.TiDBDynamicTableSource;
import com.zhihu.flink.tidb.source.TiDBRowDataInputFormat;
import com.zhihu.presto.tidb.ClientConfig;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "tidb";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    Properties properties = new Properties();
    properties.putAll(context.getCatalogTable().toProperties());
    return new TiDBDynamicTableSource(context.getCatalogTable().getSchema(), properties);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(
        ConfigOptions.key(ClientConfig.DATABASE_URL).stringType().noDefaultValue(),
        ConfigOptions.key(ClientConfig.USERNAME).stringType().noDefaultValue(),
        ConfigOptions.key(TiDBRowDataInputFormat.DATABASE_NAME).stringType().noDefaultValue(),
        ConfigOptions.key(TiDBRowDataInputFormat.TABLE_NAME).stringType().noDefaultValue()
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        ConfigOptions.key(ClientConfig.PASSWORD).stringType().noDefaultValue(),
        ConfigOptions.key(ClientConfig.MAX_POOL_SIZE).stringType().noDefaultValue(),
        ConfigOptions.key(ClientConfig.MIN_IDLE_SIZE).stringType().noDefaultValue()
    );
  }
}
