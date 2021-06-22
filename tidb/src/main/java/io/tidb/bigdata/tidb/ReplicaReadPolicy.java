package io.tidb.bigdata.tidb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.tikv.common.TiConfiguration.ReplicaRead;
import org.tikv.common.replica.Region;
import org.tikv.common.replica.ReplicaSelector;
import org.tikv.common.replica.Store;
import org.tikv.common.replica.Store.Label;

public abstract class ReplicaReadPolicy implements ReplicaSelector  {
  private final Map<String, String> labels;
  private final Set<String> whitelist;
  private final Set<String> blacklist;

  private static class LeaderReadPolicy extends ReplicaReadPolicy {
    private LeaderReadPolicy() {
      super(null, null, null);
    }

    @Override
    public List<Store> select(Region region) {
      List<Store> leader = new ArrayList<>(1);
      leader.add(region.getLeader());
      return leader;
    }

    @Override
    public ReplicaRead toReplicaRead() {
      return ReplicaRead.LEADER;
    }
  }

  public static final ReplicaReadPolicy DEFAULT = new LeaderReadPolicy();

  private static class FollowerReadPolicy extends ReplicaReadPolicy {
    private FollowerReadPolicy(final Map<String, String> labels,
        final Set<String> whitelist, final Set<String> blacklist) {
      super(labels, whitelist, blacklist);
    }

    @Override
    public List<Store> select(Region region) {
      Store leader = region.getLeader();
      Store[] stores = region.getStores();
      List<Store> followers = new ArrayList<>(stores.length);
      for (Store store : stores) {
        if (!store.equals(leader) && accept(store)) {
          followers.add(store);
        }
      }
      if (followers.isEmpty()) {
        // Fallback to leader if no follower available
        followers.add(leader);
      } else {
        Collections.shuffle(followers);
      }
      return followers;
    }

    @Override
    public ReplicaRead toReplicaRead() {
      return ReplicaRead.FOLLOWER;
    }
  }

  private static class LeaderAndFollowerReadPolicy extends ReplicaReadPolicy {
    private LeaderAndFollowerReadPolicy(final Map<String, String> labels,
        final Set<String> whitelist, final Set<String> blacklist) {
      super(labels, whitelist, blacklist);
    }

    @Override
    public List<Store> select(Region region) {
      Store leader = region.getLeader();
      Store[] stores = region.getStores();
      List<Store> candidates = new ArrayList<>(stores.length);
      for (Store store : stores) {
        if (!store.equals(leader) && accept(store)) {
          candidates.add(store);
        }
      }
      Collections.shuffle(candidates);
      if (accept(leader)) {
        candidates.add(leader);
      }
      if (candidates.isEmpty()) {
        // Fallback to leader if no candidate available
        candidates.add(leader);
      }
      return candidates;
    }

    @Override
    public ReplicaRead toReplicaRead() {
      return ReplicaRead.LEADER_AND_FOLLOWER;
    }
  }

  private ReplicaReadPolicy(final Map<String, String> labels,
      final Set<String> whitelist, final Set<String> blacklist) {
    this.labels = labels;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
  }

  private static Map<String, String> extractLabels(final Map<String, String> properties) {
    String[] labels = properties.getOrDefault(ClientConfig.TIDB_REPLICA_READ_LABEL,
        ClientConfig.TIDB_REPLICA_READ_LABEL_DEFAULT).split(",");
    Map<String, String> map = new HashMap<>(labels.length);
    String key;
    String value;
    for (String pair : labels) {
      if (pair.isEmpty()) {
        continue;
      }
      String[] pairs = pair.trim().split("=");
      if (pairs.length != 2 || (key = pairs[0].trim()).isEmpty()
          || (value = pairs[1].trim()).isEmpty()) {
        throw new IllegalArgumentException("Invalid replica read labels: "
            + Arrays.toString(labels));
      }
      map.put(key, value);
    }
    return map;
  }

  private static Set<String> extractList(final Map<String, String> properties,
      final String key, final String defaultValue) {
    return Arrays.stream(properties.getOrDefault(key, defaultValue).split(","))
        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
  }

  static ReplicaReadPolicy create(final Map<String, String> properties) {
    Map<String, String> labels = extractLabels(properties);
    Set<String> whitelist = extractList(properties,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_WHITELIST,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_DEFAULT);
    Set<String> blacklist = extractList(properties,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_BLACKLIST,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_DEFAULT);
    switch (properties.getOrDefault(ClientConfig.TIDB_REPLICA_READ,
        ClientConfig.TIDB_REPLICA_READ_DEFAULT)) {
      case ClientConfig.TIDB_REPLICA_READ_FOLLOWER:
        return new FollowerReadPolicy(labels, whitelist, blacklist);
      case ClientConfig.TIDB_REPLICA_READ_LEADER_AND_FOLLOWER:
        return new LeaderAndFollowerReadPolicy(labels, whitelist, blacklist);
      case ClientConfig.TIDB_REPLICA_READ_LEADER:
        // FALLTHROUGH
      default:
        return DEFAULT;
    }
  }

  private boolean inWhitelist(Store store) {
    if (whitelist.isEmpty()) {
      return false;
    }
    return whitelist.stream().anyMatch(a -> a.equals(store.getAddress()));
  }

  private boolean notInBlacklist(Store store) {
    if (blacklist.isEmpty()) {
      return true;
    }
    return blacklist.stream().noneMatch(a -> a.equals(store.getAddress()));
  }

  private boolean matchLabels(Store store) {
    if (labels.isEmpty()) {
      return true;
    }
    int matched = 0;
    for (Label label : store.getLabels()) {
      if (label.getValue().equals(labels.get(label.getKey()))) {
        matched++;
      }
    }
    return matched == labels.size();
  }

  protected boolean accept(Store store) {
    return !store.isLearner()
        && (matchLabels(store) || inWhitelist(store))
        && notInBlacklist(store);
  }

  public abstract ReplicaRead toReplicaRead();
}
