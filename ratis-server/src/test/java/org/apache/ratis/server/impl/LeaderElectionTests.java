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
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LAST_LEADER_ELECTION_ELAPSED_TIME;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_COUNT_METRIC;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIME_TAKEN;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIMEOUT_COUNT_METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Timer;

public abstract class LeaderElectionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    LOG.info("Running testBasicLeaderElection");
    final MiniRaftCluster cluster = newCluster(5);
    cluster.start();
    RaftTestUtil.waitAndKillLeader(cluster);
    RaftTestUtil.waitAndKillLeader(cluster);
    RaftTestUtil.waitAndKillLeader(cluster);
    testFailureCase("waitForLeader after killed a majority of servers",
        () -> RaftTestUtil.waitForLeader(cluster, null, false),
        IllegalStateException.class);
    cluster.shutdown();
  }

  @Test
  public void testChangeLeader() throws Exception {
    SegmentedRaftLogTestUtils.setRaftLogWorkerLogLevel(Level.TRACE);
    LOG.info("Running testChangeLeader");
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();

    RaftPeerId leader = RaftTestUtil.waitForLeader(cluster).getId();
    for(int i = 0; i < 10; i++) {
      leader = RaftTestUtil.changeLeader(cluster, leader, IllegalStateException::new);
      ExitUtils.assertNotTerminated();
    }
    SegmentedRaftLogTestUtils.setRaftLogWorkerLogLevel(Level.INFO);
    cluster.shutdown();
  }

  @Test
  public void testLostMajorityHeartbeats() throws Exception {
    runWithNewCluster(3, this::runTestLostMajorityHeartbeats);
  }

  void runTestLostMajorityHeartbeats(CLUSTER cluster) throws Exception {
    final TimeDuration maxTimeout = RaftServerConfigKeys.Rpc.timeoutMax(getProperties());
    final RaftServer.Division leader = waitForLeader(cluster);
    try {
      isolate(cluster, leader.getId());
      maxTimeout.sleep();
      maxTimeout.sleep();
      RaftServerTestUtil.assertLostMajorityHeartbeatsRecently(leader);
    } finally {
      deIsolate(cluster, leader.getId());
    }
  }

  @Test
  public void testTransferLeader() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        List<RaftServer.Division> followers = cluster.getFollowers();
        Assert.assertEquals(followers.size(), 2);
        RaftServer.Division newLeader = followers.get(0);

        List<RaftPeer> peers = cluster.getPeers();
        List<RaftPeer> peersWithNewPriority = getPeersWithPriority(peers, newLeader.getPeer());
        RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriority.toArray(new RaftPeer[0]));
        Assert.assertTrue(reply.isSuccess());

        reply = client.admin().transferLeadership(newLeader.getId(), 20000);
        assertTrue(reply.isSuccess());

        final RaftServer.Division currLeader = waitForLeader(cluster);
        assertTrue(newLeader.getId() == currLeader.getId());

        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertTrue(reply.getReplierId().equals(newLeader.getId().toString()));
        Assert.assertTrue(reply.isSuccess());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testLeaderLease() throws Exception {
    TimeDuration rpcTimeoutMin = RaftServerConfigKeys.Rpc.timeoutMin(getProperties());
    double ratio = RaftServerConfigKeys.Rpc.getLeaderLeaseTimeoutRatio(getProperties()) * 1.0 / 100;
    TimeDuration leaderLeaseTimeout = rpcTimeoutMin.multiply(ratio);

    try (final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        Assert.assertTrue(leader.getInfo().isLeader());
        Assert.assertTrue(leader.getInfo().isLeaderReady());

        isolate(cluster, leader.getId());
        Thread.sleep(leaderLeaseTimeout.toLong(TimeUnit.MILLISECONDS));

        Assert.assertTrue(leader.getInfo().isLeader());
        Assert.assertFalse(leader.getInfo().isLeaderReady());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testTransferLeaderTimeout() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        List<RaftServer.Division> followers = cluster.getFollowers();
        Assert.assertEquals(followers.size(), 2);
        RaftServer.Division newLeader = followers.get(0);

        // isolate new leader, so that transfer leadership will timeout
        isolate(cluster, newLeader.getId());

        List<RaftPeer> peers = cluster.getPeers();
        List<RaftPeer> peersWithNewPriority = getPeersWithPriority(peers, newLeader.getPeer());
        RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriority.toArray(new RaftPeer[0]));
        Assert.assertTrue(reply.isSuccess());

        CompletableFuture<Boolean> transferTimeoutFuture = CompletableFuture.supplyAsync(() -> {
          try {
            long timeoutMs = 5000;
            long start = System.currentTimeMillis();
            try {
              client.admin().transferLeadership(newLeader.getId(), timeoutMs);
            } catch (TransferLeadershipException e) {
              long cost = System.currentTimeMillis() - start;
              Assert.assertTrue(cost > timeoutMs);
              Assert.assertTrue(e.getMessage().contains("Failed to transfer leadership to"));
              Assert.assertTrue(e.getMessage().contains("timed out"));
            }

            return true;
          } catch (IOException e) {
            return false;
          }
        });

        // before transfer timeout, leader should in steppingDown
        JavaUtils.attemptRepeatedly(() -> {
          try {
            client.io().send(new RaftTestUtil.SimpleMessage("message"));
          } catch (LeaderSteppingDownException e) {
            Assert.assertTrue(e.getMessage().contains("is stepping down"));
          }
          return null;
        }, 5, TimeDuration.ONE_SECOND, "check leader steppingDown", RaftServer.LOG);

        Assert.assertTrue(transferTimeoutFuture.get());

        // after transfer timeout, leader should accept request
        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertTrue(reply.getReplierId().equals(leader.getId().toString()));
        Assert.assertTrue(reply.isSuccess());

        deIsolate(cluster, newLeader.getId());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testEnforceLeader() throws Exception {
    LOG.info("Running testEnforceLeader");
    final int numServer = 5;
    try(final MiniRaftCluster cluster = newCluster(numServer)) {
      cluster.start();

      final RaftPeerId firstLeader = waitForLeader(cluster).getId();
      LOG.info("firstLeader = {}", firstLeader);
      final int first = MiniRaftCluster.getIdIndex(firstLeader.toString());

      final int random = ThreadLocalRandom.current().nextInt(numServer - 1);
      final String newLeader = "s" + (random < first? random: random + 1);
      LOG.info("enforce leader to {}", newLeader);
      enforceLeader(cluster, newLeader, LOG);
    }
  }

  static void enforceLeader(MiniRaftCluster cluster, final String newLeader, Logger LOG) throws InterruptedException {
    LOG.info(cluster.printServers());
    for(int i = 0; !cluster.tryEnforceLeader(newLeader) && i < 10; i++) {
      final RaftServer.Division currLeader = cluster.getLeader();
      LOG.info("try enforcing leader to " + newLeader + " but " +
          (currLeader == null ? "no leader for round " + i : "new leader is " + currLeader.getId()));
    }
    LOG.info(cluster.printServers());

    final RaftServer.Division leader = cluster.getLeader();
    Assert.assertEquals(newLeader, leader.getId().toString());
  }

  @Test
  public void testLateServerStart() throws Exception {
    final int numServer = 3;
    LOG.info("Running testLateServerStart");
    final MiniRaftCluster cluster = newCluster(numServer);
    cluster.initServers();

    // start all except one servers
    final Iterator<RaftServer> i = cluster.getServers().iterator();
    for(int j = 1; j < numServer; j++) {
      i.next().start();
    }

    final RaftServer.Division leader = waitForLeader(cluster);
    final TimeDuration sleepTime = TimeDuration.valueOf(3, TimeUnit.SECONDS);
    LOG.info("sleep " + sleepTime);
    sleepTime.sleep();

    // start the last server
    final RaftServerProxy lastServer = (RaftServerProxy) i.next();
    lastServer.start();
    final RaftPeerId lastServerLeaderId = JavaUtils.attemptRepeatedly(
        () -> Optional.ofNullable(lastServer.getImpls().iterator().next().getState().getLeaderId())
            .orElseThrow(() -> new IllegalStateException("No leader yet")),
        10, ONE_SECOND, "getLeaderId", LOG);
    LOG.info(cluster.printServers());
    Assert.assertEquals(leader.getId(), lastServerLeaderId);
  }

  protected void testDisconnectLeader() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Thread.sleep(1000);
        isolate(cluster, leader.getId());
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertNotEquals(reply.getReplierId(), leader.getId().toString());
        Assert.assertTrue(reply.isSuccess());
      } finally {
        deIsolate(cluster, leader.getId());
      }

      cluster.shutdown();
    }
  }

  private void isolate(MiniRaftCluster cluster, RaftPeerId id) {
    try {
      BlockRequestHandlingInjection.getInstance().blockReplier(id.toString());
      cluster.setBlockRequestsFrom(id.toString(), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void deIsolate(MiniRaftCluster cluster, RaftPeerId id) {
    BlockRequestHandlingInjection.getInstance().unblockReplier(id.toString());
    cluster.setBlockRequestsFrom(id.toString(), false);
  }

  @Test
  public void testLeaderElectionMetrics() throws IOException, InterruptedException {
    Timestamp timestamp = Timestamp.currentTime();
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();
    final RaftServer.Division leaderServer = waitForLeader(cluster);

    final RatisMetricRegistry ratisMetricRegistry = LeaderElectionMetrics.getMetricRegistryForLeaderElection(
        leaderServer.getMemberId());

    // Verify each metric individually.
    long numLeaderElections = ratisMetricRegistry.counter(LEADER_ELECTION_COUNT_METRIC).getCount();
    assertTrue(numLeaderElections > 0);

    long numLeaderElectionTimeout = ratisMetricRegistry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).getCount();
    assertTrue(numLeaderElectionTimeout > 0);

    Timer timer = ratisMetricRegistry.timer(LEADER_ELECTION_TIME_TAKEN);
    double meanTimeNs = timer.getSnapshot().getMean();
    long elapsedNs = timestamp.elapsedTime().toLong(TimeUnit.NANOSECONDS);
    assertTrue(timer.getCount() > 0 && meanTimeNs < elapsedNs);
    Long leaderElectionLatency = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(LAST_LEADER_ELECTION_ELAPSED_TIME)).values().iterator().next().getValue();
    assertTrue(leaderElectionLatency > 0L);
  }

  @Test
  public void testImmediatelyRevertedToFollower() {
    RaftServerImpl server = createMockServer(true);
    LeaderElection subject = new LeaderElection(server, false);

    try {
      subject.startInForeground();
      assertEquals(LifeCycle.State.CLOSED, subject.getCurrentState());
    } catch (Exception e) {
      LOG.info("Error starting LeaderElection", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testShutdownBeforeStart() {
    RaftServerImpl server = createMockServer(false);
    LeaderElection subject = new LeaderElection(server, false);

    try {
      subject.shutdown();
      subject.startInForeground();
      assertEquals(LifeCycle.State.CLOSED, subject.getCurrentState());
    } catch (Exception e) {
      LOG.info("Error starting LeaderElection", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testPreVote() {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      RaftServer.Division leader = waitForLeader(cluster);

      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        final List<RaftServer.Division> followers = cluster.getFollowers();
        assertEquals(followers.size(), 2);

        RaftServer.Division follower = followers.get(0);
        isolate(cluster, follower.getId());
        // send message so that the isolated follower's log lag the others
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertTrue(reply.isSuccess());

        final long savedTerm = leader.getInfo().getCurrentTerm();
        LOG.info("Wait follower {} timeout and trigger pre-vote", follower.getId());
        Thread.sleep(2000);
        deIsolate(cluster, follower.getId());
        Thread.sleep(2000);
        // with pre-vote leader will not step down
        RaftServer.Division newleader = waitForLeader(cluster);
        assertNotNull(newleader);
        assertEquals(newleader.getId(), leader.getId());
        // with pre-vote, term will not change
        assertEquals(savedTerm, leader.getInfo().getCurrentTerm());

        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertTrue(reply.isSuccess());
      }

      cluster.shutdown();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static RaftServerImpl createMockServer(boolean alive) {
    final DivisionInfo info = mock(DivisionInfo.class);
    when(info.isAlive()).thenReturn(alive);
    when(info.isCandidate()).thenReturn(false);
    RaftServerImpl server = mock(RaftServerImpl.class);
    when(server.getInfo()).thenReturn(info);
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("any"), RaftGroupId.randomId());
    when(server.getMemberId()).thenReturn(memberId);
    LeaderElectionMetrics leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(memberId, () -> 0);
    when(server.getLeaderElectionMetrics()).thenReturn(leaderElectionMetrics);
    RaftServerProxy proxy = mock(RaftServerProxy.class);
    RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, true);
    when(proxy.getProperties()).thenReturn(properties);
    when(server.getRaftServer()).thenReturn(proxy);
    return server;
  }
}
