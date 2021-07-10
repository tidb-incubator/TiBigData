package io.tidb.bigdata.tidb;

import java.net.URI;
import java.net.URISyntaxException;
import org.tikv.common.HostMapping;

/**
 * Host mapping implementation that add extra dns search suffix
 */
public class DnsSearchHostMapping implements HostMapping {
  private final String dnsSearch;

  public DnsSearchHostMapping(String dnsSearch) {
    if (dnsSearch != null && !dnsSearch.isEmpty()) {
      this.dnsSearch = "." + dnsSearch;
    } else {
      this.dnsSearch = "";
    }
  }

  @Override
  public URI getMappedURI(URI uri) {
    if (!uri.getHost().endsWith(dnsSearch)) {
      try {
        return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost() + dnsSearch,
            uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException ex) {
        throw new IllegalArgumentException(ex);
      }
    } else {
      return uri;
    }
  }
}
