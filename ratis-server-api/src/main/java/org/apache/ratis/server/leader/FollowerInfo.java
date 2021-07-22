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
package org.apache.ratis.server.leader;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information of a follower, provided the local server is the Leader
 */
public interface FollowerInfo {
  Logger LOG = LoggerFactory.getLogger(FollowerInfo.class);

  /** @return the name of this object. */
  String getName();

  /** @return this follower's peer info. */
  RaftPeer getPeer();

  /** @return the matchIndex acknowledged by this follower. */
  long getMatchIndex();

  /** Update this follower's matchIndex. */
  boolean updateMatchIndex(long newMatchIndex);

  /** @return the commitIndex acknowledged by this follower. */
  long getCommitIndex();

  /** Update follower's commitIndex. */
  boolean updateCommitIndex(long newCommitIndex);

  /** @return the snapshotIndex acknowledged by this follower. */
  long getSnapshotIndex();

  /** Set follower's snapshotIndex. */
  void setSnapshotIndex(long newSnapshotIndex);

  /** @return the nextIndex for this follower. */
  long getNextIndex();

  /** Increase the nextIndex for this follower. */
  void increaseNextIndex(long newNextIndex);

  /** Decrease the nextIndex for this follower. */
  void decreaseNextIndex(long newNextIndex);

  /** Set the nextIndex for this follower. */
  void setNextIndex(long newNextIndex);

  /** Update the nextIndex for this follower. */
  void updateNextIndex(long newNextIndex);

  /** @return the lastRpcResponseTime . */
  Timestamp getLastRpcResponseTime();

  /** Update lastRpcResponseTime to the current time. */
  void updateLastRpcResponseTime(Timestamp sendTime);

  /** Update lastHeartBeatSendTime to the current time. */
  void updateLastHeartBeatSendTime();

  /** @return the lastRpcSendTimeWithResponse . */
  Timestamp getLastRpcSendTimeWithResponse();

  /** @return the lastHeartBeatSendTime . */
  Timestamp getLastHeartBeatSendTime();
}
