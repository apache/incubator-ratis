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
import org.apache.ratis.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The peer configuration of a raft cluster.
 *
 * The objects of this class are immutable.
 */
class PeerConfiguration {
  private final Map<RaftPeerId, RaftPeer> peers;
  private final Map<RaftPeerId, RaftPeer> listeners;

  PeerConfiguration(Iterable<RaftPeer> peers) {
    this(peers, Collections.emptyList());
  }

  PeerConfiguration(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
    Objects.requireNonNull(peers);
    Objects.requireNonNull(listeners);
    Map<RaftPeerId, RaftPeer> peerMap = new HashMap<>();
    for(RaftPeer p : peers) {
      final RaftPeer previous = peerMap.putIfAbsent(p.getId(), p);
      if (previous != null) {
        throw new IllegalArgumentException("Found duplicated ids " + p.getId() + " in peers " + peers);
      }
    }
    this.peers = Collections.unmodifiableMap(peerMap);

    Map<RaftPeerId, RaftPeer> listenerMap = new HashMap<>();
    for(RaftPeer p : listeners) {
      if (peerMap.containsKey(p.getId())
          || listenerMap.putIfAbsent(p.getId(), p) != null) {
        throw new IllegalArgumentException("Found duplicated ids " + p.getId() + " in listeners " + listeners);
      }
    }
    this.listeners = Collections.unmodifiableMap(listenerMap);
  }

  Collection<RaftPeer> getPeers() {
    return Collections.unmodifiableCollection(peers.values());
  }

  Collection<RaftPeer> getAllPeers() {
    Stream<RaftPeer> combinedStream = Stream.of(peers.values(), listeners.values())
        .flatMap(Collection::stream);
    Collection<RaftPeer> peersCombined =
        combinedStream.collect(Collectors.toList());
    return Collections.unmodifiableCollection(peersCombined);
  }

  int size() {
    return peers.size();
  }

  @Override
  public String toString() {
    return peers.values().toString();
  }

  RaftPeer getPeer(RaftPeerId id) {
    return peers.get(id);
  }

  boolean contains(RaftPeerId id) {
    return peers.containsKey(id);
  }

  List<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    List<RaftPeer> others = new ArrayList<>();
    for (Map.Entry<RaftPeerId, RaftPeer> entry : peers.entrySet()) {
      if (!selfId.equals(entry.getValue().getId())) {
        others.add(entry.getValue());
      }
    }
    return others;
  }

  boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    int num = 0;
    if (contains(selfId)) {
      num++;
    }
    for (RaftPeerId other : others) {
      if (contains(other)) {
        num++;
      }
    }
    return num > size() / 2;
  }

  boolean majorityRejectVotes(Collection<RaftPeerId> rejected) {
    int num = size();
    for (RaftPeerId other : rejected) {
      if (contains(other)) {
        num --;
      }
    }
    return num <= size() / 2;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final PeerConfiguration that = (PeerConfiguration)obj;
    return this.peers.equals(that.peers);
  }

  @Override
  public int hashCode() {
    return peers.keySet().hashCode(); // hashCode of a set is well defined in Java.
  }
}
