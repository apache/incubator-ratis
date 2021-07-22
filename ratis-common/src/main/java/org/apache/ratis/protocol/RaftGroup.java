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
package org.apache.ratis.protocol;

import org.apache.ratis.util.Preconditions;

import java.util.*;

/**
 * Description of a raft group, which has a unique {@link RaftGroupId} and a collection of {@link RaftPeer}.
 *
 * The objects of this class are immutable.
 */
public final class RaftGroup {
  private static final RaftGroup EMPTY_GROUP = new RaftGroup();

  public static RaftGroup emptyGroup() {
    return EMPTY_GROUP;
  }

  /** @return a group with the given id and peers. */
  public static RaftGroup valueOf(RaftGroupId groupId, RaftPeer... peers) {
    return new RaftGroup(groupId, peers == null || peers.length == 0? Collections.emptyList(): Arrays.asList(peers));
  }

  /** @return a group with the given id and peers. */
  public static RaftGroup valueOf(RaftGroupId groupId, Collection<RaftPeer> peers) {
    return new RaftGroup(groupId, peers);
  }

  public static RaftGroup valueOf(RaftGroupId groupId, Collection<RaftPeer> peers, Collection<RaftPeer> learners) {
    return new RaftGroup(groupId, peers, learners);
  }

  /** The group id */
  private final RaftGroupId groupId;
  /** The group of raft peers */
  private final Map<RaftPeerId, RaftPeer> peers;
  private final Map<RaftPeerId, RaftPeer> listeners;

  private RaftGroup() {
    this.groupId = RaftGroupId.emptyGroupId();
    this.peers = Collections.emptyMap();
    this.listeners = Collections.emptyMap();
  }

  private RaftGroup(RaftGroupId groupId, Collection<RaftPeer> peers) {
    this(groupId, peers, Collections.emptyList());
  }

  private RaftGroup(RaftGroupId groupId, Collection<RaftPeer> peers, Collection<RaftPeer> listeners) {
    this.groupId = Objects.requireNonNull(groupId, "groupId == null");
    Preconditions.assertTrue(!groupId.equals(EMPTY_GROUP.getGroupId()),
        () -> "Group Id " + groupId + " is reserved for the empty group.");

    if (peers == null || peers.isEmpty()) {
      this.peers = Collections.emptyMap();
    } else {
      final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      peers.forEach(p -> map.put(p.getId(), p));
      this.peers = Collections.unmodifiableMap(map);
    }

    if (listeners == null || listeners.isEmpty()) {
      this.listeners = Collections.emptyMap();
    } else {
      final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      listeners.forEach(p -> map.put(p.getId(), p));
      this.listeners = Collections.unmodifiableMap(map);
    }
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public Collection<RaftPeer> getPeers() {
    return peers.values();
  }

  public Collection<RaftPeer> getListeners() {
    return listeners.values();
  }

  /** @return the peer with the given id if it is in this group; otherwise, return null. */
  public RaftPeer getPeer(RaftPeerId id) {
    return peers.get(Objects.requireNonNull(id, "id == null"));
  }

  public RaftPeer getListener(RaftPeerId id) {
    return listeners.get(Objects.requireNonNull(id, "id == null"));
  }

  public boolean isListener(RaftPeerId id) {
    return listeners.containsKey(id);
  }

  @Override
  public int hashCode() {
    return groupId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof RaftGroup)) {
      return false;
    }
    final RaftGroup that = (RaftGroup)obj;
    return this.groupId.equals(that.groupId) && this.peers.equals(that.peers);
  }

  @Override
  public String toString() {
    return groupId + ":" + peers.values();
  }
}
