package com.zhihu.tibigdata.flink.tidb;

import static com.zhihu.tibigdata.tidb.ClientConfig.DATABASE_URL;
import static com.zhihu.tibigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static com.zhihu.tibigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static com.zhihu.tibigdata.tidb.ClientConfig.PASSWORD;
import static com.zhihu.tibigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN;
import static com.zhihu.tibigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static com.zhihu.tibigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static com.zhihu.tibigdata.tidb.ClientConfig.USERNAME;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

public class TiDBCatalogFactory implements CatalogFactory {

  public static final String CATALOG_TYPE_VALUE_TIDB = "tidb";

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    return new TiDBCatalog(name, properties);
  }

  @Override
  public Map<String, String> requiredContext() {
    return ImmutableMap.of(
        CATALOG_TYPE, CATALOG_TYPE_VALUE_TIDB,
        CATALOG_PROPERTY_VERSION, "1"
    );
  }

  @Override
  public List<String> supportedProperties() {
    return ImmutableList.of(
        USERNAME,
        PASSWORD,
        DATABASE_URL,
        MAX_POOL_SIZE,
        MIN_IDLE_SIZE,
        TIDB_WRITE_MODE,
        TIDB_REPLICA_READ,
        TIDB_FILTER_PUSH_DOWN
    );
  }
}
