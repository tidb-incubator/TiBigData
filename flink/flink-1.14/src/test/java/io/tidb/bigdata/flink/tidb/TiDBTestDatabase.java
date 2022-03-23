package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.FlinkTestBase.DATABASE_NAME;

import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBTestDatabase extends ExternalResource {

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override
  protected void before() throws Throwable {
    ClientSession clientSession = ClientSession.create(
        new ClientConfig(ConfigUtils.defaultProperties()));
    clientSession.sqlUpdate(String.format("CREATE DATABASE IF NOT EXISTS `%s`", DATABASE_NAME));
    logger.info("Create database {}", DATABASE_NAME);
  }

  @Override
  protected void after() {
    ClientSession clientSession = ClientSession.create(
        new ClientConfig(ConfigUtils.defaultProperties()));
    clientSession.sqlUpdate(String.format("DROP DATABASE IF EXISTS `%s`", DATABASE_NAME));
    logger.info("Drop database {}", DATABASE_NAME);
  }
}