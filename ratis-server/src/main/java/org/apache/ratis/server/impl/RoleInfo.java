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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintain the Role of a Raft Peer.
 */
class RoleInfo {
  public static final Logger LOG = LoggerFactory.getLogger(RoleInfo.class);

  private final RaftPeerId id;
  private final boolean isListener;
  private volatile RaftPeerRole role;
  /** Used when the peer is leader */
  private final AtomicReference<LeaderStateImpl> leaderState = new AtomicReference<>();
  /** Used when the peer is follower, to monitor election timeout */
  private final AtomicReference<FollowerState> followerState = new AtomicReference<>();
  /** Used when the peer is candidate, to request votes from other peers */
  private final AtomicReference<LeaderElection> leaderElection = new AtomicReference<>();

  private final AtomicReference<Timestamp> transitionTime;

  RoleInfo(RaftPeerId id, boolean isListener) {
    this.id = id;
    this.isListener = isListener;
    this.transitionTime = new AtomicReference<>(Timestamp.currentTime());
  }

  boolean isListener() {
    return this.isListener;
  }

  void transitionRole(RaftPeerRole newRole) {
    this.role = newRole;
    this.transitionTime.set(Timestamp.currentTime());
  }

  long getRoleElapsedTimeMs() {
    return transitionTime.get().elapsedTimeMs();
  }

  RaftPeerRole getCurrentRole() {
    return role;
  }

  boolean isLeaderReady() {
    return getLeaderState().map(LeaderStateImpl::isReady).orElse(false);
  }

  Optional<LeaderStateImpl> getLeaderState() {
    return Optional.ofNullable(leaderState.get());
  }

  LeaderStateImpl getLeaderStateNonNull() {
    return Objects.requireNonNull(leaderState.get(), "leaderState is null");
  }

  LogEntryProto startLeaderState(RaftServerImpl server) {
    return updateAndGet(leaderState, new LeaderStateImpl(server)).start();
  }

  void shutdownLeaderState(boolean allowNull) {
    final LeaderStateImpl leader = leaderState.getAndSet(null);
    if (leader == null) {
      if (!allowNull) {
        throw new NullPointerException("leaderState == null");
      }
    } else {
      LOG.info("{}: shutdown {}", id, leader);
      leader.stop();
    }
    // TODO: make sure that StateMachineUpdater has applied all transactions that have context
  }

  Optional<FollowerState> getFollowerState() {
    return Optional.ofNullable(followerState.get());
  }

  void startFollowerState(RaftServerImpl server, Object reason) {
    updateAndGet(followerState, new FollowerState(server, reason)).start();
  }

  void shutdownFollowerState() {
    final FollowerState follower = followerState.getAndSet(null);
    if (follower != null) {
      LOG.info("{}: shutdown {}", id, follower);
      follower.stopRunning();
      follower.interrupt();
    }
  }

  void startLeaderElection(RaftServerImpl server, boolean force) {
    updateAndGet(leaderElection, new LeaderElection(server, force)).start();
  }

  void shutdownLeaderElection() {
    final LeaderElection election = leaderElection.getAndSet(null);
    if (election != null) {
      LOG.info("{}: shutdown {}", id, election);
      election.shutdown();
      // no need to interrupt the election thread
    }
  }

  private <T> T updateAndGet(AtomicReference<T> ref, T current) {
    final T updated = ref.updateAndGet(previous -> previous != null? previous: current);
    Preconditions.assertTrue(updated == current, "previous != null");
    LOG.info("{}: start {}", id, current);
    return updated;
  }

  @Override
  public String toString() {
    return String.format("%9s", role);
  }
}
