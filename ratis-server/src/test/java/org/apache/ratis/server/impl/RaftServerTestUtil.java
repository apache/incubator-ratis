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

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.server.DataStreamServer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class RaftServerTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(RaftServerTestUtil.class);

  public static final RaftGroupMemberId TEST_MEMBER_ID = RaftGroupMemberId.valueOf(
      RaftPeerId.valueOf("test"), RaftGroupId.emptyGroupId());

  public static DelayLocalExecutionInjection getLogSyncDelay() {
    return new DelayLocalExecutionInjection(RaftServerImpl.LOG_SYNC);
  }

  public static void setStateMachineUpdaterLogLevel(Level level) {
    Log4jUtils.setLogLevel(StateMachineUpdater.LOG, level);
  }
  public static void setWatchRequestsLogLevel(Level level) {
    Log4jUtils.setLogLevel(WatchRequests.LOG, level);
  }
  public static void setPendingRequestsLogLevel(Level level) {
    Log4jUtils.setLogLevel(PendingRequests.LOG, level);
  }

  public static void waitAndCheckNewConf(MiniRaftCluster cluster,
      RaftPeer[] peers, int numOfRemovedPeers, Collection<RaftPeerId> deadPeers)
      throws Exception {
    final TimeDuration sleepTime = cluster.getTimeoutMax().apply(n -> n * (numOfRemovedPeers + 2));
    JavaUtils.attempt(() -> waitAndCheckNewConf(cluster, Arrays.asList(peers), deadPeers),
        10, sleepTime, "waitAndCheckNewConf", LOG);
  }
  private static void waitAndCheckNewConf(MiniRaftCluster cluster,
      Collection<RaftPeer> peers, Collection<RaftPeerId> deadPeers) {
    LOG.info("waitAndCheckNewConf: peers={}, deadPeers={}, {}", peers, deadPeers, cluster.printServers());
    Assert.assertNotNull(cluster.getLeader());

    int numIncluded = 0;
    int deadIncluded = 0;
    final RaftConfigurationImpl current = RaftConfigurationImpl.newBuilder()
        .setConf(peers).setLogEntryIndex(0).build();
    for (RaftServer.Division d : cluster.iterateDivisions()) {
      final RaftServerImpl server = (RaftServerImpl)d;
      LOG.info("checking {}", server);
      if (deadPeers != null && deadPeers.contains(server.getId())) {
        if (current.containsInConf(server.getId())) {
          deadIncluded++;
        }
        continue;
      }
      final RaftConfigurationImpl conf = server.getState().getRaftConf();
      if (current.containsInConf(server.getId())) {
        numIncluded++;
        Assert.assertTrue(conf.isStable());
        Assert.assertTrue(conf.hasNoChange(peers));
      } else if (server.getInfo().isAlive()) {
        // The server is successfully removed from the conf
        // It may not be shutdown since it may not be able to talk to the new leader (who is not in its conf).
        Assert.assertTrue(conf.isStable());
        Assert.assertFalse(conf.containsInConf(server.getId()));
      }
    }
    Assert.assertEquals(peers.size(), numIncluded + deadIncluded);
  }

  public static long getNextIndex(RaftServer.Division server) {
    return ((RaftServerImpl)server).getState().getNextIndex();
  }

  public static long getLatestInstalledSnapshotIndex(RaftServer.Division server) {
    return ((RaftServerImpl)server).getState().getLatestInstalledSnapshotIndex();
  }

  static ServerState getState(RaftServer.Division server) {
    return ((RaftServerImpl)server).getState();
  }

  public static ConfigurationManager getConfigurationManager(RaftServer.Division server) {
    return (ConfigurationManager) RaftTestUtil.getDeclaredField(getState(server), "configurationManager");
  }

  public static RaftConfiguration newRaftConfiguration(Collection<RaftPeer> peers) {
    return RaftConfigurationImpl.newBuilder().setConf(peers).build();
  }

  public static RaftServerRpc getServerRpc(RaftServer.Division server) {
    return ((RaftServerImpl)server).getRaftServer().getServerRpc();
  }

  private static Optional<LeaderStateImpl> getLeaderState(RaftServer.Division server) {
    return ((RaftServerImpl)server).getRole().getLeaderState();
  }

  public static Stream<LogAppender> getLogAppenders(RaftServer.Division server) {
    return getLeaderState(server).map(LeaderStateImpl::getLogAppenders).orElse(null);
  }

  public static void restartLogAppenders(RaftServer.Division server) {
    final LeaderStateImpl leaderState = getLeaderState(server).orElseThrow(
        () -> new IllegalStateException(server + " is not the leader"));
    leaderState.getLogAppenders().forEach(leaderState::restart);
  }

  public static RaftServer.Division getDivision(RaftServer server, RaftGroupId groupId) {
    return JavaUtils.callAsUnchecked(() -> server.getDivision(groupId));
  }

  public static DataStreamServer newDataStreamServer(RaftServer server) {
    return new DataStreamServerImpl(server, null);
  }

  public static DataStreamMap newDataStreamMap(Object name) {
    return new DataStreamMapImpl(name);
  }

  public static CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftServer.Division server, RaftClientRequest request) {
    return ((RaftServerImpl)server).streamEndOfRequestAsync(request);
  }

  public static void assertLostMajorityHeartbeatsRecently(RaftServer.Division leader) {
    final FollowerState f = ((RaftServerImpl)leader).getRole().getFollowerState().orElse(null);
    Assert.assertNotNull(f);
    Assert.assertTrue(f.lostMajorityHeartbeatsRecently());
  }

  public static SegmentedRaftLog newSegmentedRaftLog(RaftGroupMemberId memberId, DivisionInfo info,
      RaftStorage storage, RaftProperties properties) {
    final RaftServerImpl server = Mockito.mock(RaftServerImpl.class);
    Mockito.when(server.getInfo()).thenReturn(info);

    return new SegmentedRaftLog(memberId, server, null,
        server::notifyTruncatedLogEntry,
        server::submitUpdateCommitEvent,
        storage, () -> -1, properties);
  }

  public static boolean isHighestPriority(RaftConfiguration config, RaftPeerId peerId) {
    return ((RaftConfigurationImpl)config).isHighestPriority(peerId);
  }
}
