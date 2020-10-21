/*
 * Copyright 2020 Zhihu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zhihu.tibigdata.jdbc;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

public abstract class LoadBalanceDriver implements Driver {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoadBalanceDriver.class);

  /**
   * implements {@link java.util.function.Function}, Default: {@link DefaultUrlSelector}
   */
  private final Function<List<String>, String> urlSelector;

  private final Driver driver;

  public LoadBalanceDriver(String balanceDriverName, Function<List<String>, String> urlSelector)
      throws SQLException {
    this.urlSelector = requireNonNull(urlSelector, "urlSelector can not be null");
    requireNonNull(balanceDriverName, "driver name can not be null");
    try {
      this.driver = (Driver) Class.forName(balanceDriverName).newInstance();
    } catch (Exception e) {
      throw new SQLException("can not create driver", e);
    }
  }

  public LoadBalanceDriver(String balanceDriverName) throws SQLException {
    this(balanceDriverName, new DefaultUrlSelector());
  }

  /**
   * urls split by ',', like mysql:jdbc://host1:port1/database,mysql:jdbc://host2:port2/database
   */
  @Override
  public Connection connect(String urls, Properties info) throws SQLException {
    List<String> urlList = getUrlList(urls);
    while (urlList.size() > 0) {
      String url = urlSelector.apply(urlList);
      LOG.debug("try connect to " + url);
      try {
        return driver.connect(url, info);
      } catch (Exception e) {
        LOG.warn("connect to " + url + " fail, retry other url", e);
        urlList.remove(url);
      }
    }
    throw new SQLException("can not get connection");
  }

  @Override
  public boolean acceptsURL(String urls) throws SQLException {
    for (String s : getUrlList(urls)) {
      if (driver.acceptsURL(s)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return driver.getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    return driver.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return driver.getMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return driver.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return driver.getParentLogger();
  }

  // return a mutable url list
  private List<String> getUrlList(String urls) {
    return Arrays.stream(requireNonNull(urls, "urls can not be null").split(","))
        .collect(Collectors.toList());
  }
}
