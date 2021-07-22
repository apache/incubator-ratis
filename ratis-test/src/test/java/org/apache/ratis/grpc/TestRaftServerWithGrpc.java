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
package org.apache.ratis.grpc;

import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_READ_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_STALE_READ_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_WATCH_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_WRITE_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.REQUEST_QUEUE_LIMIT_HIT_COUNTER;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.REQUEST_MEGA_BYTE_SIZE;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RESOURCE_LIMIT_HIT_COUNTER;

import com.codahale.metrics.Gauge;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StreamException;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.GrpcClientProtocolClient;
import org.apache.ratis.grpc.client.GrpcClientProtocolService;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class TestRaftServerWithGrpc extends BaseTest implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Log4jUtils.setLogLevel(GrpcClientProtocolService.LOG, Level.ALL);
    Log4jUtils.setLogLevel(GrpcClientProtocolClient.LOG, Level.ALL);
  }

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);
    RaftClientConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(1, TimeUnit.SECONDS));
  }

  @Test
  public void testServerRestartOnException() throws Exception {
    runWithNewCluster(1, this::runTestServerRestartOnException);
  }

  static RaftServer newRaftServer(MiniRaftClusterWithGrpc cluster, RaftPeerId id, StateMachine stateMachine,
      RaftProperties p) throws IOException {
    final RaftGroup group = cluster.getGroup();
    return RaftServer.newBuilder()
        .setServerId(id)
        .setGroup(cluster.getGroup())
        .setStateMachine(stateMachine)
        .setProperties(p)
        .setParameters(cluster.setPropertiesAndInitParameters(id, group, p))
        .build();
  }

  void runTestServerRestartOnException(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    final RaftProperties p = getProperties();
    GrpcConfigKeys.Server.setPort(p, RaftServerTestUtil.getServerRpc(leader).getInetSocketAddress().getPort());

    // Create a raft server proxy with server rpc bound to a different address
    // compared to leader. This helps in locking the raft storage directory to
    // be used by next raft server proxy instance.
    final StateMachine stateMachine = cluster.getLeader().getStateMachine();
    RaftServerConfigKeys.setStorageDir(p, Collections.singletonList(cluster.getStorageDir(leaderId)));
    newRaftServer(cluster, leaderId, stateMachine, p);
    // Close the server rpc for leader so that new raft server can be bound to it.
    RaftServerTestUtil.getServerRpc(cluster.getLeader()).close();

    // Create a raft server proxy with server rpc bound to same address as
    // the leader. This step would fail as the raft storage has been locked by
    // the raft server proxy created earlier. Raft server proxy should close
    // the rpc server on failure.
    RaftServerConfigKeys.setStorageDir(p, Collections.singletonList(cluster.getStorageDir(leaderId)));
    testFailureCase("start a new server with the same address",
        () -> newRaftServer(cluster, leaderId, stateMachine, p).start(),
        IOException.class, OverlappingFileLockException.class);
    // Try to start a raft server rpc at the leader address.
    cluster.getServerFactory(leaderId).newRaftServerRpc(cluster.getServer(leaderId));
  }

  @Test
  public void testUnsupportedMethods() throws Exception {
    runWithNewCluster(1, this::runTestUnsupportedMethods);
  }

  void runTestUnsupportedMethods(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    final RaftServerRpc rpc = cluster.getServerFactory(leaderId).newRaftServerRpc(cluster.getServer(leaderId));

    testFailureCase("appendEntries",
        () -> rpc.appendEntries(null),
        UnsupportedOperationException.class);

    testFailureCase("installSnapshot",
        () -> rpc.installSnapshot(null),
        UnsupportedOperationException.class);
  }

  @Test
  public void testLeaderRestart() throws Exception {
    runWithNewCluster(3, this::runTestLeaderRestart);
  }

  void runTestLeaderRestart(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);

    try (final RaftClient client = cluster.createClient()) {
      // send a request to make sure leader is ready
      final CompletableFuture<RaftClientReply> f = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f.get().isSuccess());
    }

    try (final RaftClient client = cluster.createClient()) {
      final RaftClientRpc rpc = client.getClientRpc();

      final AtomicLong seqNum = new AtomicLong();
      {
        // send a request using rpc directly
        final RaftClientRequest request = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
        final CompletableFuture<RaftClientReply> f = rpc.sendRequestAsync(request);
        Assert.assertTrue(f.get().isSuccess());
      }

      // send another request which will be blocked
      final SimpleStateMachine4Testing stateMachine = SimpleStateMachine4Testing.get(leader);
      stateMachine.blockStartTransaction();
      final RaftClientRequest requestBlocked = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
      final CompletableFuture<RaftClientReply> futureBlocked = rpc.sendRequestAsync(requestBlocked);

      // change leader
      RaftTestUtil.changeLeader(cluster, leader.getId());
      Assert.assertNotEquals(RaftPeerRole.LEADER, leader.getInfo().getCurrentRole());

      // the blocked request should fail
      testFailureCase("request should fail", futureBlocked::get,
          ExecutionException.class, AlreadyClosedException.class);
      stateMachine.unblockStartTransaction();

      // send one more request which should timeout.
      final RaftClientRequest requestTimeout = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
      rpc.handleException(leader.getId(), new Exception(), true);
      final CompletableFuture<RaftClientReply> f = rpc.sendRequestAsync(requestTimeout);
      testFailureCase("request should timeout", f::get,
          ExecutionException.class, TimeoutIOException.class);
    }

  }

  @Test
  public void testRaftClientMetrics() throws Exception {
    runWithNewCluster(3, this::testRaftClientRequestMetrics);
  }

  @Test
  public void testRaftServerMetrics() throws Exception {
    final RaftProperties p = getProperties();
    RaftServerConfigKeys.Write.setElementLimit(p, 10);
    RaftServerConfigKeys.Write.setByteLimit(p, SizeInBytes.valueOf("1MB"));
    try {
      runWithNewCluster(3, this::testRequestMetrics);
    } finally {
      RaftServerConfigKeys.Write.setElementLimit(p, RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
      RaftServerConfigKeys.Write.setByteLimit(p, RaftServerConfigKeys.Write.BYTE_LIMIT_DEFAULT);
    }
  }

  void testRequestMetrics(MiniRaftClusterWithGrpc cluster) throws Exception {

    try (RaftClient client = cluster.createClient()) {
      // send a request to make sure leader is ready
      final CompletableFuture< RaftClientReply > f = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f.get().isSuccess());
    }

    SimpleStateMachine4Testing stateMachine = SimpleStateMachine4Testing.get(cluster.getLeader());
    stateMachine.blockFlushStateMachineData();

    String message = "2nd Message";
    // Block stateMachine flush data, so that 2nd request will not be
    // completed, and so it will not be removed from pending request map.
    List<RaftClient> clients = new ArrayList<>();

    try {
      RaftClient client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
      clients.add(client);
      client.async().send(new SimpleMessage(message));


      final SortedMap<String, Gauge> gaugeMap = getRaftServerMetrics(cluster.getLeader())
          .getRegistry().getGauges((s, metric) -> s.contains(
              REQUEST_MEGA_BYTE_SIZE));


      for (int i = 0; i < 10; i++) {
        client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
        clients.add(client);
        client.async().send(new SimpleMessage(message));
      }

      // Because we have passed 11 requests, and the element queue size is 10.
      RaftTestUtil.waitFor(() -> getRaftServerMetrics(cluster.getLeader())
          .getCounter(REQUEST_QUEUE_LIMIT_HIT_COUNTER).getCount() == 1, 300, 5000);

      stateMachine.unblockFlushStateMachineData();

      // Send a message with 1025kb , our byte size limit is 1024kb (1mb) , so it should fail
      // and byte size counter limit will be hit.

      client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
      client.async().send(new SimpleMessage(RandomStringUtils
          .random(SizeInBytes.valueOf("1025kb").getSizeInt(), true, false)));
      clients.add(client);

      RaftTestUtil.waitFor(() -> getRaftServerMetrics(cluster.getLeader())
              .getCounter(REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER).getCount() == 1, 300, 5000);

      Assert.assertEquals(2, getRaftServerMetrics(cluster.getLeader())
              .getCounter(RESOURCE_LIMIT_HIT_COUNTER).getCount());
    } finally {
      for (RaftClient client : clients) {
        client.close();
      }
    }
  }

  static RaftServerMetricsImpl getRaftServerMetrics(RaftServer.Division division) {
    return (RaftServerMetricsImpl) division.getRaftServerMetrics();
  }

  void testRaftClientRequestMetrics(MiniRaftClusterWithGrpc cluster) throws IOException,
      ExecutionException, InterruptedException {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftServerMetricsImpl raftServerMetrics = getRaftServerMetrics(leader);

    try (final RaftClient client = cluster.createClient()) {
      final CompletableFuture<RaftClientReply> f1 = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f1.get().isSuccess());
      Assert.assertTrue(raftServerMetrics.getTimer(RAFT_CLIENT_WRITE_REQUEST).getCount() > 0);

      final CompletableFuture<RaftClientReply> f2 = client.async().sendReadOnly(new SimpleMessage("testing"));
      Assert.assertTrue(f2.get().isSuccess());
      Assert.assertTrue(raftServerMetrics.getTimer(RAFT_CLIENT_READ_REQUEST).getCount() > 0);

      final CompletableFuture<RaftClientReply> f3 = client.async().sendStaleRead(new SimpleMessage("testing"),
          0, leader.getId());
      Assert.assertTrue(f3.get().isSuccess());
      Assert.assertTrue(raftServerMetrics.getTimer(RAFT_CLIENT_STALE_READ_REQUEST).getCount() > 0);

      final CompletableFuture<RaftClientReply> f4 = client.async().watch(0, RaftProtos.ReplicationLevel.ALL);
      Assert.assertTrue(f4.get().isSuccess());
      Assert.assertTrue(raftServerMetrics.getTimer(String.format(RAFT_CLIENT_WATCH_REQUEST, "-ALL")).getCount() > 0);

      final CompletableFuture<RaftClientReply> f5 = client.async().watch(0, RaftProtos.ReplicationLevel.MAJORITY);
      Assert.assertTrue(f5.get().isSuccess());
      Assert.assertTrue(raftServerMetrics.getTimer(String.format(RAFT_CLIENT_WATCH_REQUEST, "")).getCount() > 0);
    }
  }

  @Test
  public void TestStreamEndOfRequestAsync() throws Exception {
    runWithNewCluster(1, this::runTestStreamEndOfRequestAsync);
  }

  void runTestStreamEndOfRequestAsync(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final RaftGroupId leaderGroupId = leader.getGroup().getGroupId();
    final RaftClient client = cluster.createClient();
    final AtomicLong seqNum = new AtomicLong();
    final RaftClientRequest clientRequest = newRaftClientRequest(client, leaderId, seqNum.incrementAndGet(),
            RaftClientRequest.messageStreamRequestType(12, 12, true));

    // Leader completes exceptionally, because there is no such stream
    // Creating realistic stream is complex, since streams are created by clients, but
    // this function tests server functionality.
    CompletableFuture<RaftClientRequest> fRequest = RaftServerTestUtil.streamEndOfRequestAsync(leader, clientRequest);
    Assert.assertNotNull(fRequest);
    Assert.assertTrue(fRequest.isCompletedExceptionally());
    fRequest.exceptionally(e -> {
      Assert.assertSame(e.getCause().getClass(), StreamException.class);
      return clientRequest;
    });

    // On non leader, request should fail because only leaders handle this kind of requests
    RaftServer server = cluster.putNewServer(RaftPeerId.getRaftPeerId("Server 21"), leader.getGroup(), false);
    fRequest = RaftServerTestUtil.streamEndOfRequestAsync(server.getDivision(leaderGroupId), clientRequest);
    Assert.assertNotNull(fRequest);
    Assert.assertTrue(fRequest.isCompletedExceptionally());
    fRequest.exceptionally(e -> {
      Assert.assertSame(e.getCause().getClass(), Exception.class);
      return clientRequest;
    });
  }

  static RaftClientRequest newRaftClientRequest(RaftClient client, RaftPeerId serverId, long seqNum) {
    return newRaftClientRequest(client, serverId, seqNum, RaftClientRequest.writeRequestType());
  }

  static RaftClientRequest newRaftClientRequest(RaftClient client, RaftPeerId serverId, long seqNum,
                                                RaftClientRequest.Type type) {
    final SimpleMessage m = new SimpleMessage("m" + seqNum);
    return RaftClientTestUtil.newRaftClientRequest(client, serverId, seqNum, m,
            type, ProtoUtils.toSlidingWindowEntry(seqNum, seqNum == 1L));
  }
}
