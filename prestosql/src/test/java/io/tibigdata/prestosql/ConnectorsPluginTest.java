package io.tibigdata.prestosql;

import static com.google.common.collect.Iterables.getOnlyElement;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import io.tidb.bigdata.prestosql.ConnectorsPlugin;
import io.tidb.bigdata.test.IntegrationTest;
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
