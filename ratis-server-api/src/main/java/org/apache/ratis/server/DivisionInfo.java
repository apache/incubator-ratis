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

package org.apache.ratis.server;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.util.LifeCycle;

/**
 * Information of a raft server division.
 */
public interface DivisionInfo {
  /** @return the current role of this server division. */
  RaftPeerRole getCurrentRole();

  /** Is this server division currently a follower? */
  default boolean isFollower() {
    return getCurrentRole() == RaftPeerRole.FOLLOWER;
  }

  /** Is this server division currently a learner? */
  default boolean isLearner() {
    return getCurrentRole() == RaftPeerRole.LISTENER;
  }

  /** Is this server division currently a candidate? */
  default boolean isCandidate() {
    return getCurrentRole() == RaftPeerRole.CANDIDATE;
  }

  /** Is this server division currently the leader? */
  default boolean isLeader() {
    return getCurrentRole() == RaftPeerRole.LEADER;
  }

  /** Is this server division currently the leader and ready? */
  boolean isLeaderReady();

  /** @return the life cycle state of this server division. */
  LifeCycle.State getLifeCycleState();

  /** Is this server division alive? */
  default boolean isAlive() {
    return !getLifeCycleState().isClosingOrClosed();
  }

  /** @return the role information of this server division. */
  RoleInfoProto getRoleInfoProto();

  /** @return the current term of this server division. */
  long getCurrentTerm();

  /** @return the last log index already applied by the state machine of this server division. */
  long getLastAppliedIndex();

  /**
   * @return an array of next indices of the followers if this server division is the leader;
   *         otherwise, return null.
   */
  long[] getFollowerNextIndices();
}
