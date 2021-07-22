/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The configuration of the raft cluster.
 *
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 *
 * The objects of this class are immutable.
 */
final class RaftConfigurationImpl implements RaftConfiguration {
  /** Create a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  static final class Builder {
    private PeerConfiguration oldConf;
    private PeerConfiguration conf;
    private long logEntryIndex = RaftLog.INVALID_LOG_INDEX;

    private boolean forceStable = false;
    private boolean forceTransitional = false;

    private Builder() {}

    Builder setConf(PeerConfiguration conf) {
      Objects.requireNonNull(conf);
      Preconditions.assertTrue(this.conf == null, "conf is already set.");
      this.conf = conf;
      return this;
    }

    Builder setConf(Iterable<RaftPeer> peers) {
      return setConf(new PeerConfiguration(peers));
    }

    Builder setConf(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
      return setConf(new PeerConfiguration(peers, listeners));
    }

    Builder setConf(RaftConfigurationImpl transitionalConf) {
      Objects.requireNonNull(transitionalConf);
      Preconditions.assertTrue(transitionalConf.isTransitional());

      Preconditions.assertTrue(!forceTransitional);
      forceStable = true;
      return setConf(transitionalConf.conf);
    }


    Builder setOldConf(PeerConfiguration oldConf) {
      Objects.requireNonNull(oldConf);
      Preconditions.assertTrue(this.oldConf == null, "oldConf is already set.");
      this.oldConf = oldConf;
      return this;
    }

    Builder setOldConf(Iterable<RaftPeer> oldPeers) {
      return setOldConf(new PeerConfiguration(oldPeers));
    }

    Builder setOldConf(RaftConfigurationImpl stableConf) {
      Objects.requireNonNull(stableConf);
      Preconditions.assertTrue(stableConf.isStable());

      Preconditions.assertTrue(!forceStable);
      forceTransitional = true;
      return setOldConf(stableConf.conf);
    }

    Builder setLogEntryIndex(long logEntryIndex) {
      Preconditions.assertTrue(logEntryIndex != RaftLog.INVALID_LOG_INDEX);
      Preconditions.assertTrue(this.logEntryIndex == RaftLog.INVALID_LOG_INDEX, "logEntryIndex is already set.");
      this.logEntryIndex = logEntryIndex;
      return this;
    }

    RaftConfigurationImpl build() {
      if (forceTransitional) {
        Preconditions.assertTrue(oldConf != null);
      }
      if (forceStable) {
        Preconditions.assertTrue(oldConf == null);
      }
      return new RaftConfigurationImpl(conf, oldConf, logEntryIndex);
    }
  }

  /** Non-null only if this configuration is transitional. */
  private final PeerConfiguration oldConf;
  /**
   * The current peer configuration while this configuration is stable;
   * or the new peer configuration while this configuration is transitional.
   */
  private final PeerConfiguration conf;

  /** The index of the corresponding log entry for this configuration. */
  private final long logEntryIndex;

  private RaftConfigurationImpl(PeerConfiguration conf, PeerConfiguration oldConf,
      long logEntryIndex) {
    this.conf = Objects.requireNonNull(conf);
    this.oldConf = oldConf;
    this.logEntryIndex = logEntryIndex;
  }

  /** Is this configuration transitional, i.e. in the middle of a peer change? */
  boolean isTransitional() {
    return oldConf != null;
  }

  /** Is this configuration stable, i.e. no on-going peer change? */
  boolean isStable() {
    return oldConf == null;
  }

  boolean containsInConf(RaftPeerId peerId) {
    return conf.contains(peerId);
  }

  boolean isHighestPriority(RaftPeerId peerId) {
    RaftPeer target = getPeer(peerId);
    if (target == null) {
      return false;
    }
    Collection<RaftPeer> peers = getCurrentPeers();
    for (RaftPeer peer : peers) {
      if (peer.getPriority() >= target.getPriority() && !peer.equals(target)) {
        return false;
      }
    }
    return true;
  }

  boolean containsInOldConf(RaftPeerId peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  /**
   * @return true iff the given peer is contained in conf and,
   *         if old conf exists, is contained in old conf.
   */
  boolean containsInBothConfs(RaftPeerId peerId) {
    return containsInConf(peerId) &&
        (oldConf == null || containsInOldConf(peerId));
  }

  @Override
  public RaftPeer getPeer(RaftPeerId id) {
    if (id == null) {
      return null;
    }
    RaftPeer peer = conf.getPeer(id);
    if (peer != null) {
      return peer;
    } else if (oldConf != null) {
      return oldConf.getPeer(id);
    }
    return null;
  }

  @Override
  public Collection<RaftPeer> getAllPeers() {
    final Collection<RaftPeer> allPeers = new ArrayList<>(conf.getAllPeers());
    if (oldConf != null) {
      oldConf.getAllPeers().stream().filter(p -> !allPeers.contains(p))
          .forEach(allPeers::add);
    }
    return allPeers;
  }

  /**
   * @return all the peers other than the given self id from the conf,
   *         and the old conf if it exists.
   */
  Collection<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      oldConf.getOtherPeers(selfId).stream()
          .filter(p -> !others.contains(p))
          .forEach(others::add);
    }
    return others;
  }

  /** @return true if the self id together with the others are in the majority. */
  boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  /** @return true if the rejects are in the majority(maybe half is enough in some cases). */
  boolean majorityRejectVotes(Collection<RaftPeerId> rejects) {
    return conf.majorityRejectVotes(rejects) ||
            (oldConf != null && oldConf.majorityRejectVotes(rejects));
  }

  @Override
  public String toString() {
    return logEntryIndex + ": " + conf + ", old=" + oldConf;
  }

  boolean hasNoChange(Collection<RaftPeer> newMembers) {
    if (!isStable() || conf.size() != newMembers.size()) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      if (!conf.contains(peer.getId()) || conf.getPeer(peer.getId()).getPriority() != peer.getPriority()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long getLogEntryIndex() {
    return logEntryIndex;
  }

  /** @return the peers which are not contained in conf. */
  Collection<RaftPeer> filterNotContainedInConf(List<RaftPeer> peers) {
    return peers.stream().filter(p -> !containsInConf(p.getId())).collect(Collectors.toList());
  }

  @Override
  public Collection<RaftPeer> getPreviousPeers() {
    return oldConf != null ? oldConf.getPeers() : Collections.emptyList();
  }

  @Override
  public Collection<RaftPeer> getCurrentPeers() {
    return conf.getPeers();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final RaftConfigurationImpl that = (RaftConfigurationImpl)obj;
    return this.logEntryIndex == that.logEntryIndex
        && Objects.equals(this.conf,  that.conf)
        && Objects.equals(this.oldConf,  that.oldConf);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(logEntryIndex);
  }
}
