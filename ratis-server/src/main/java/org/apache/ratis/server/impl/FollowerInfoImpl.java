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

import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.util.Timestamp;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class FollowerInfoImpl implements FollowerInfo {
  private final String name;
  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;

  private final RaftPeer peer;
  private final AtomicReference<Timestamp> lastRpcResponseTime;
  private final AtomicReference<Timestamp> lastRpcSendTime;
  private final AtomicReference<Timestamp> lastRpcSendTimeWithResponse;
  private final RaftLogIndex nextIndex;
  private final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 0L);
  private final RaftLogIndex commitIndex = new RaftLogIndex("commitIndex", RaftLog.INVALID_LOG_INDEX);
  private final RaftLogIndex snapshotIndex = new RaftLogIndex("snapshotIndex", 0L);
  private volatile boolean attendVote;

  FollowerInfoImpl(RaftGroupMemberId id, RaftPeer peer, Timestamp lastRpcTime, long nextIndex, boolean attendVote) {
    this.name = id + "->" + peer.getId();
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.peer = peer;
    this.lastRpcResponseTime = new AtomicReference<>(lastRpcTime);
    this.lastRpcSendTime = new AtomicReference<>(lastRpcTime);
    this.lastRpcSendTimeWithResponse = new AtomicReference<>(lastRpcTime);
    this.nextIndex = new RaftLogIndex("nextIndex", nextIndex);
    this.attendVote = attendVote;
  }

  @Override
  public long getMatchIndex() {
    return matchIndex.get();
  }

  @Override
  public boolean updateMatchIndex(long newMatchIndex) {
    return matchIndex.updateToMax(newMatchIndex, debugIndexChange);
  }

  @Override
  public long getCommitIndex() {
    return commitIndex.get();
  }

  @Override
  public boolean updateCommitIndex(long newCommitIndex) {
    return commitIndex.updateToMax(newCommitIndex, debugIndexChange);
  }

  @Override
  public long getSnapshotIndex() {
    return snapshotIndex.get();
  }

  @Override
  public long getNextIndex() {
    return nextIndex.get();
  }

  @Override
  public void increaseNextIndex(long newNextIndex) {
    nextIndex.updateIncreasingly(newNextIndex, debugIndexChange);
  }

  @Override
  public void decreaseNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> old <= 0L? old: Math.min(old - 1, newNextIndex), infoIndexChange);
  }

  @Override
  public void setNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> newNextIndex >= 0 ? newNextIndex : old, infoIndexChange);
  }

  @Override
  public void updateNextIndex(long newNextIndex) {
    nextIndex.updateToMax(newNextIndex, infoIndexChange);
  }

  @Override
  public void setSnapshotIndex(long newSnapshotIndex) {
    snapshotIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    matchIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    nextIndex.setUnconditionally(newSnapshotIndex + 1, infoIndexChange);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name + "(c" + getCommitIndex() + ",m" + getMatchIndex() + ",n" + getNextIndex()
        + ", attendVote=" + attendVote +
        ", lastRpcSendTime=" + lastRpcSendTime.get().elapsedTimeMs() +
        ", lastRpcResponseTime=" + lastRpcResponseTime.get().elapsedTimeMs() + ")";
  }

  void startAttendVote() {
    attendVote = true;
  }

  boolean isAttendingVote() {
    return attendVote;
  }

  @Override
  public RaftPeer getPeer() {
    return peer;
  }

  @Override
  public void updateLastRpcResponseTime() {
    lastRpcResponseTime.set(Timestamp.currentTime());
  }

  @Override
  public Timestamp getLastRpcResponseTime() {
    return lastRpcResponseTime.get();
  }

  @Override
  public void updateLastRpcSendTime() {
    lastRpcSendTime.set(Timestamp.currentTime());
  }

  @Override
  public void updateLastRpcSendTimeWithResponse(Timestamp time) {
    lastRpcSendTimeWithResponse.set(time);
  }

  @Override
  public Timestamp getLastRpcSendTimeWithResponse() {
    return lastRpcSendTimeWithResponse.get();
  }

  @Override
  public Timestamp getLastRpcTime() {
    return Timestamp.latest(lastRpcResponseTime.get(), lastRpcSendTime.get());
  }
}
