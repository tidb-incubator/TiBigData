package io.tidb.bigdata.tidb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.StoreVersion;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiBatchWriteException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.txn.TTLManager;
import org.tikv.txn.TwoPhaseCommitter;

public class TiDBWriteHelper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBWriteHelper.class);

  private static final int PREWRITE_BACKOFFER_MS = 240000;
  private static final int MIN_DELAY_CLEAN_TABLE_LOCK = 60000;
  private static final int DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA = 30000;
  private static final int PRIMARY_KEY_COMMIT_BACKOFF =
      MIN_DELAY_CLEAN_TABLE_LOCK - DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA;

  private final TiSession session;
  private final long startTs;
  private final TwoPhaseCommitter twoPhaseCommitter;
  private final boolean isTtlUpdate;
  private byte[] primaryKey;

  private TTLManager ttlManager;

  public TiDBWriteHelper(TiSession session, long startTs) {
    this.session = session;
    this.startTs = startTs;
    this.twoPhaseCommitter = new TwoPhaseCommitter(session, startTs);
    this.isTtlUpdate = StoreVersion.minTiKVVersion("3.0.5", session.getPDClient());
  }

  public TiDBWriteHelper(TiSession session, long startTs, byte[] primaryKey) {
    this(session, startTs);
    this.primaryKey = primaryKey;
  }


  private Pair<List<byte[]>, List<byte[]>> transformKeyValue(List<BytePairWrapper> pairs) {
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();
    pairs.forEach(pair -> {
      keys.add(pair.getKey());
      values.add(pair.getValue());
    });
    return new Pair<>(keys, values);
  }

  public void preWriteFirst(@Nonnull BytePairWrapper pairWrapper) {
    preWriteFirst(Collections.singletonList(pairWrapper));
  }

  public void preWriteFirst(@Nonnull List<BytePairWrapper> pairs) {
    if (primaryKey != null) {
      throw new TiKVException("Primary key exists");
    }
    Iterator<BytePairWrapper> iterator = pairs.iterator();
    if (!iterator.hasNext()) {
      throw new TiKVException("Empty keys for pre-write");
    }
    BytePairWrapper primaryPair = iterator.next();
    this.primaryKey = primaryPair.getKey();
    byte[] primaryValue = primaryPair.getValue();
    BackOffer prewritePrimaryBackoff = ConcreteBackOffer.newCustomBackOff(PREWRITE_BACKOFFER_MS);
    LOG.info("start to pre-write primaryKey");
    // pre-write primary keys
    twoPhaseCommitter.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey, primaryValue);
    LOG.info("Pre-write primaryKey success");
    // write other as secondary keys
    preWriteSecondKeys(iterator);
  }

  public void preWriteSecondKeys(@Nonnull List<BytePairWrapper> pairs) {
    if (primaryKey == null) {
      throw new TiKVException("Primary key is null");
    }
    Iterator<BytePairWrapper> iterator = pairs.iterator();
    preWriteSecondKeys(iterator);
  }

  public void preWriteSecondKeys(@Nonnull Iterator<BytePairWrapper> iterator) {
    if (!iterator.hasNext()) {
      return;
    }
    // start primary key ttl update
    if (isTtlUpdate && ttlManager == null) {
      ttlManager = new TTLManager(session, startTs, primaryKey);
      ttlManager.keepAlive();
    }

    LOG.info("Start to pre-write secondary keys");
    // pre-write secondary keys
    twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, iterator, PREWRITE_BACKOFFER_MS);
    LOG.info("Pre-write secondary keys success");
  }

  public void commitPrimaryKey() {
    long commitTs = session.getTimestamp().getVersion();
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
          "Invalid transaction tso with startTs=" + startTs + ", commitTs=" + commitTs);
    }
    BackOffer commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(PRIMARY_KEY_COMMIT_BACKOFF);
    LOG.info("Start to commit primaryKey");
    // commit primary keys
    twoPhaseCommitter.commitPrimaryKey(commitPrimaryBackoff, getPrimaryKeyMust(), commitTs);
    LOG.info("Commit primaryKey success");
  }


  public Optional<byte[]> getPrimaryKey() {
    return Optional.ofNullable(primaryKey);
  }

  public byte[] getPrimaryKeyMust() {
    return Objects.requireNonNull(primaryKey, "Primary key is null!");
  }

  @Override
  public void close() {
    // stop primary key ttl update
    if (isTtlUpdate && ttlManager != null) {
      try {
        ttlManager.close();
      } catch (Exception e) {
        LOG.warn("Close ttlManager failed", e);
      }
    }
  }
}