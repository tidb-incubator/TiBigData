/*
 *
 * Copyright 2019 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.operation.iterator;

import static java.util.Objects.requireNonNull;

import io.tidb.bigdata.tidb.key.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.KeyException;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.shade.com.google.protobuf.ByteString;

public class ConcreteScanIterator extends ScanIterator {

  private final long version;
  private final Logger logger = LoggerFactory.getLogger(ConcreteScanIterator.class);

  public ConcreteScanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    // Passing endKey as ByteString.EMPTY means that endKey is +INF by default,
    super(conf, builder, startKey, endKey, Integer.MAX_VALUE);
    this.version = version;
  }

  @Override
  TiRegion loadCurrentRegionToCache() throws GrpcException {
    TiRegion region;
    try (RegionStoreClient client = builder.build(startKey)) {
      client.setTimeout(conf.getScanTimeout());
      region = client.getRegion();
      BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
      currentCache = client.scan(backOffer, startKey, version);
      return region;
    }
  }

  private ByteString resolveCurrentLock(KvPair current) {
    logger.warn(String.format("resolve current key error %s", current.getError().toString()));
    Pair<TiRegion, TiStore> pair =
        builder.getRegionManager().getRegionStorePairByKey(current.getKey());
    TiRegion region = pair.first;
    TiStore store = pair.second;
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    try (RegionStoreClient client = builder.build(region, store)) {
      return client.get(backOffer, current.getKey(), version);
    } catch (Exception e) {
      throw new KeyException(current.getError());
    }
  }

  /**
   * Cache is drained when - no data extracted - scan limit was not defined - have read the last
   * index of cache - index not initialized
   *
   * @return whether cache is drained
   */
  private boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
  }

  private boolean notEndOfScan() {
    return limit > 0
        && !(processingLastBatch
            && (index >= currentCache.size()
                || Key.toRawKey(currentCache.get(index).getKey()).compareTo(endKey) >= 0));
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    // continue when cache is empty but not null
    while (currentCache != null && currentCache.isEmpty()) {
      if (isCacheDrained() && cacheLoadFails()) {
        return false;
      }
    }
    return notEndOfScan();
  }

  private KvPair getCurrent() {
    --limit;
    KvPair current = currentCache.get(index++);

    requireNonNull(current, "current kv pair cannot be null");
    if (current.hasError()) {
      ByteString val = resolveCurrentLock(current);
      current = KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
    }

    return current;
  }

  @Override
  public KvPair next() {
    return getCurrent();
  }
}
