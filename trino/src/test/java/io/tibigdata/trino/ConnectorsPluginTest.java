package io.tibigdata.trino;

import static com.google.common.collect.Iterables.getOnlyElement;

import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.trino.ConnectorsPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ConnectorsPluginTest {

  @Test
  public void testCreateConnector() {
    Plugin plugin = new ConnectorsPlugin();
    ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
    factory.create("tidb", ConfigUtils.getProperties(), new TestingConnectorContext());
  }

}
