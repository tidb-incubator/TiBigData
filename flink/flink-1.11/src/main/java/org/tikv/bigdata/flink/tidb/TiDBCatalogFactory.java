package org.tikv.bigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.table.catalog.Catalog;

public class TiDBCatalogFactory extends TiDBBaseCatalogFactory {

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    return new TiDBCatalog(name, properties);
  }
}
