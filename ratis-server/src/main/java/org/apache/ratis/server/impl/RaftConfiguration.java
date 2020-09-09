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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.Preconditions;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * The configuration of the raft cluster.
 *
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 *
 * The objects of this class are immutable.
 */
public final class RaftConfiguration {
  /** Create a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftConfiguration} objects. */
  public static final class Builder {
    private PeerConfiguration oldConf;
    private PeerConfiguration conf;
    private long logEntryIndex = RaftServerConstants.INVALID_LOG_INDEX;

    private boolean forceStable = false;
    private boolean forceTransitional = false;

    private Builder() {}

    public Builder setConf(PeerConfiguration conf) {
      Objects.requireNonNull(conf);
      Preconditions.assertTrue(this.conf == null, "conf is already set.");
      this.conf = conf;
      return this;
    }

    public Builder setConf(Iterable<RaftPeer> peers) {
      return setConf(new PeerConfiguration(peers));
    }

    Builder setConf(RaftConfiguration transitionalConf) {
      Objects.requireNonNull(transitionalConf);
      Preconditions.assertTrue(transitionalConf.isTransitional());

      Preconditions.assertTrue(!forceTransitional);
      forceStable = true;
      return setConf(transitionalConf.conf);
    }


    public Builder setOldConf(PeerConfiguration oldConf) {
      Objects.requireNonNull(oldConf);
      Preconditions.assertTrue(this.oldConf == null, "oldConf is already set.");
      this.oldConf = oldConf;
      return this;
    }

    public Builder setOldConf(Iterable<RaftPeer> oldPeers) {
      return setOldConf(new PeerConfiguration(oldPeers));
    }

    Builder setOldConf(RaftConfiguration stableConf) {
      Objects.requireNonNull(stableConf);
      Preconditions.assertTrue(stableConf.isStable());

      Preconditions.assertTrue(!forceStable);
      forceTransitional = true;
      return setOldConf(stableConf.conf);
    }

    public Builder setLogEntryIndex(long logEntryIndex) {
      Preconditions.assertTrue(
          logEntryIndex != RaftServerConstants.INVALID_LOG_INDEX);
      Preconditions.assertTrue(
          this.logEntryIndex == RaftServerConstants.INVALID_LOG_INDEX,
          "logEntryIndex is already set.");
      this.logEntryIndex = logEntryIndex;
      return this;
    }

    /** Build a {@link RaftConfiguration}. */
    public RaftConfiguration build() {
      if (forceTransitional) {
        Preconditions.assertTrue(oldConf != null);
      }
      if (forceStable) {
        Preconditions.assertTrue(oldConf == null);
      }
      return new RaftConfiguration(conf, oldConf, logEntryIndex);
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

  private RaftConfiguration(PeerConfiguration conf, PeerConfiguration oldConf,
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

  boolean containsInOldConf(RaftPeerId peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  /**
   * @return true iff the given peer is contained in conf and,
   *         if old conf exists, is contained in old conf.
   */
  boolean contains(RaftPeerId peerId) {
    return containsInConf(peerId) &&
        (oldConf == null || containsInOldConf(peerId));
  }

  /**
   * @return the peer corresponding to the given id;
   *         or return null if the peer is not in this configuration.
   */
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

  /** @return all the peers from the conf, and the old conf if it exists. */
  public Collection<RaftPeer> getPeers() {
    final Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    if (oldConf != null) {
      oldConf.getPeers().stream().filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  /**
   * @return all the peers other than the given self id from the conf,
   *         and the old conf if it exists.
   */
  public Collection<RaftPeer> getOtherPeers(RaftPeerId selfId) {
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

  @Override
  public String toString() {
    return logEntryIndex + ": " + conf + ", old=" + oldConf;
  }

  boolean hasNoChange(Collection<RaftPeer> newMembers) {
    if (!isStable() || conf.size() != newMembers.size()) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      if (!conf.contains(peer.getId())) {
        return false;
      }
    }
    return true;
  }

  long getLogEntryIndex() {
    return logEntryIndex;
  }

  /** @return the peers which are not contained in conf. */
  Collection<RaftPeer> filterNotContainedInConf(List<RaftPeer> peers) {
    return peers.stream().filter(p -> !containsInConf(p.getId())).collect(Collectors.toList());
  }

  RaftPeer getRandomPeer(RaftPeerId exclusiveId) {
    final List<RaftPeer> peers = conf.getOtherPeers(exclusiveId);
    if (peers.isEmpty()) {
      return null;
    }
    final int index = ThreadLocalRandom.current().nextInt(peers.size());
    return peers.get(index);
  }

  Collection<RaftPeer> getPeersInOldConf() {
    return oldConf != null ? oldConf.getPeers() : Collections.emptyList();
  }

  Collection<RaftPeer> getPeersInConf() {
    return conf.getPeers();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final RaftConfiguration that = (RaftConfiguration)obj;
    return this.logEntryIndex == that.logEntryIndex
        && Objects.equals(this.conf,  that.conf)
        && Objects.equals(this.oldConf,  that.oldConf);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(logEntryIndex);
  }
}
