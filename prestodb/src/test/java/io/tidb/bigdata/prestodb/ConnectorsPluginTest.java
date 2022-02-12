package io.tidb.bigdata.prestodb;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
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
