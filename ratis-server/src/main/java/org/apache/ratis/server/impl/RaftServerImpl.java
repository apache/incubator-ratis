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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CandidateInfoProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.LeaderInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.ServerRpcProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientProtocol;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.ReconfigurationInProgressException;
import org.apache.ratis.protocol.ServerNotReadyException;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.StaleReadException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.JmxRegister;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.LifeCycle.State;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.INCONSISTENCY;
import static org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.NOT_LEADER;
import static org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS;
import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

import com.codahale.metrics.Timer;

public class RaftServerImpl implements RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerImpl.class);

  private static final String CLASS_NAME = RaftServerImpl.class.getSimpleName();
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";

  private final RaftServerProxy proxy;
  private final StateMachine stateMachine;
  private final int minTimeoutMs;
  private final int maxTimeoutMs;
  private final int rpcSlownessTimeoutMs;
  private final int sleepDeviationThresholdMs;
  private final boolean installSnapshotEnabled;

  private final LifeCycle lifeCycle;
  private final ServerState state;
  private final Supplier<RaftPeer> peerSupplier = JavaUtils.memoize(() ->
      new RaftPeer(getId(), getServerRpc().getInetSocketAddress()));
  private final RoleInfo role;

  private final RetryCache retryCache;
  private final CommitInfoCache commitInfoCache = new CommitInfoCache();

  private final RaftServerJmxAdapter jmxAdapter;
  private final LeaderElectionMetrics leaderElectionMetrics;
  private final RaftServerMetrics raftServerMetrics;

  private AtomicReference<TermIndex> inProgressInstallSnapshotRequest;

  // To avoid append entry before complete start() method
  // For example, if thread1 start(), but before thread1 startAsFollower(), thread2 receive append entry
  // request, and change state to RUNNING by lifeCycle.compareAndTransition(STARTING, RUNNING),
  // then thread1 execute lifeCycle.transition(RUNNING) in startAsFollower(),
  // So happens IllegalStateException: ILLEGAL TRANSITION: RUNNING -> RUNNING,
  private AtomicBoolean startComplete;

  RaftServerImpl(RaftGroup group, StateMachine stateMachine, RaftServerProxy proxy) throws IOException {
    final RaftPeerId id = proxy.getId();
    LOG.info("{}: new RaftServerImpl for {} with {}", id, group, stateMachine);
    this.lifeCycle = new LifeCycle(id);
    this.stateMachine = stateMachine;
    this.role = new RoleInfo(id);

    final RaftProperties properties = proxy.getProperties();
    minTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMin(properties).toIntExact(TimeUnit.MILLISECONDS);
    maxTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMax(properties).toIntExact(TimeUnit.MILLISECONDS);
    rpcSlownessTimeoutMs = RaftServerConfigKeys.Rpc.slownessTimeout(properties).toIntExact(TimeUnit.MILLISECONDS);
    sleepDeviationThresholdMs = RaftServerConfigKeys.sleepDeviationThreshold(properties);
    installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(properties);
    Preconditions.assertTrue(maxTimeoutMs > minTimeoutMs,
        "max timeout: %s, min timeout: %s", maxTimeoutMs, minTimeoutMs);
    this.proxy = proxy;

    this.state = new ServerState(id, group, properties, this, stateMachine);
    this.retryCache = initRetryCache(properties);
    this.inProgressInstallSnapshotRequest = new AtomicReference<>(null);

    this.jmxAdapter = new RaftServerJmxAdapter();
    this.leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(this);
    this.raftServerMetrics = RaftServerMetrics.getRaftServerMetrics(this);

    this.startComplete = new AtomicBoolean(false);
  }

  private RetryCache initRetryCache(RaftProperties prop) {
    final TimeDuration expireTime = RaftServerConfigKeys.RetryCache.expiryTime(prop);
    return new RetryCache(expireTime);
  }

  LogAppender newLogAppender(
      LeaderState leaderState, FollowerInfo f) {
    return getProxy().getFactory().newLogAppender(this, leaderState, f);
  }

  RaftPeer getPeer() {
    return peerSupplier.get();
  }

  int getMinTimeoutMs() {
    return minTimeoutMs;
  }

  int getMaxTimeoutMs() {
    return maxTimeoutMs;
  }

  int getRpcSlownessTimeoutMs() {
    return rpcSlownessTimeoutMs;
  }

  int getRandomTimeoutMs() {
    return minTimeoutMs + ThreadLocalRandom.current().nextInt(maxTimeoutMs - minTimeoutMs + 1);
  }

  int getSleepDeviationThresholdMs() {
    return sleepDeviationThresholdMs;
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @VisibleForTesting
  public RetryCache getRetryCache() {
    return retryCache;
  }

  public RaftServerProxy getProxy() {
    return proxy;
  }

  public RaftServerRpc getServerRpc() {
    return proxy.getServerRpc();
  }

  private void setRole(RaftPeerRole newRole, Object reason) {
    LOG.info("{}: changes role from {} to {} at term {} for {}",
        getMemberId(), this.role, newRole, state.getCurrentTerm(), reason);
    this.role.transitionRole(newRole);
  }

  boolean start() {
    if (!lifeCycle.compareAndTransition(NEW, STARTING)) {
      return false;
    }
    RaftConfiguration conf = getRaftConf();
    if (conf != null && conf.contains(getId())) {
      LOG.info("{}: start as a follower, conf={}", getMemberId(), conf);
      startAsFollower();
    } else {
      LOG.info("{}: start with initializing state, conf={}", getMemberId(), conf);
      startInitializing();
    }

    registerMBean(getId(), getMemberId().getGroupId(), jmxAdapter, jmxAdapter);
    state.start();
    startComplete.compareAndSet(false, true);
    return true;
  }

  static boolean registerMBean(
      RaftPeerId id, RaftGroupId groupdId, RaftServerMXBean mBean, JmxRegister jmx) {
    final String prefix = "Ratis:service=RaftServer,group=" + groupdId + ",id=";
    final String registered = jmx.register(mBean, Arrays.asList(
        () -> prefix + id,
        () -> prefix + ObjectName.quote(id.toString())));
    return registered != null;
  }

  /**
   * The peer belongs to the current configuration, should start as a follower
   */
  private void startAsFollower() {
    setRole(RaftPeerRole.FOLLOWER, "startAsFollower");
    role.startFollowerState(this);
    lifeCycle.transition(RUNNING);
  }

  /**
   * The peer does not have any configuration (maybe it will later be included
   * in some configuration). Start still as a follower but will not vote or
   * start election.
   */
  private void startInitializing() {
    setRole(RaftPeerRole.FOLLOWER, "startInitializing");
    // do not start FollowerState
  }

  public ServerState getState() {
    return state;
  }

  public RaftGroupMemberId getMemberId() {
    return getState().getMemberId();
  }

  public RaftPeerId getId() {
    return getMemberId().getPeerId();
  }

  RoleInfo getRole() {
    return role;
  }

  RaftConfiguration getRaftConf() {
    return getState().getRaftConf();
  }

  RaftGroup getGroup() {
    return RaftGroup.valueOf(getMemberId().getGroupId(), getRaftConf().getPeers());
  }

  /**
   * This removes the group from the server.
   * If the deleteDirectory flag is set to false, and renameDirectory
   * the directory is moved to
   * {@link RaftServerConfigKeys#REMOVED_GROUPS_DIR_KEY} location.
   * If the deleteDirectory flag is true, the group is permanently deleted.
   * @param deleteDirectory
   * @param renameDirectory
   */
  public void groupRemove(boolean deleteDirectory, boolean renameDirectory) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();

    /* Shutdown is triggered here inorder to avoid any locked files. */
    shutdown();
    if (deleteDirectory) {
      for (int i = 0; i < FileUtils.NUM_ATTEMPTS; i ++) {
        try {
          FileUtils.deleteFully(dir.getRoot());
          LOG.info("{}: Succeed to remove RaftStorageDirectory {}", getMemberId(), dir);
          break;
        } catch (NoSuchFileException e) {
          LOG.warn("{}: Some file does not exist {}", getMemberId(), dir, e);
        } catch (Exception ignored) {
          LOG.error("{}: Failed to remove RaftStorageDirectory {}", getMemberId(), dir, ignored);
          break;
        }
      }
    } else if(renameDirectory) {
      try {
        /* Create path with current group in REMOVED_GROUPS_DIR_KEY location */
        File toBeRemovedGroupFolder = new File(RaftServerConfigKeys
            .removedGroupsDir(proxy.getProperties()),
            dir.getRoot().getName());

        FileUtils.moveDirectory(dir.getRoot().toPath(),
            toBeRemovedGroupFolder.toPath());

        LOG.info("{}: Group {} is renamed successfully", getMemberId(), getGroup());
      } catch (IOException e) {
        LOG.warn("{}: Failed to remove group {}", getMemberId(),
            dir.getRoot().getName(), e);
      }
    }
  }

  public void shutdown() {
    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: shutdown", getMemberId());
      try {
        jmxAdapter.unregister();
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to un-register RaftServer JMX bean", getMemberId(), ignored);
      }
      try {
        role.shutdownFollowerState();
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to shutdown FollowerState", getMemberId(), ignored);
      }
      try{
        role.shutdownLeaderElection();
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to shutdown LeaderElection", getMemberId(), ignored);
      }
      try{
        role.shutdownLeaderState(true);
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to shutdown LeaderState monitor", getMemberId(), ignored);
      }
      try{
        state.close();
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to close state", getMemberId(), ignored);
      }
      try {
        leaderElectionMetrics.unregister();
        raftServerMetrics.unregister();
        RaftServerMetrics.removeRaftServerMetrics(this);
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to unregister metric", getMemberId(), ignored);
      }
    });
  }

  public boolean isAlive() {
    return !lifeCycle.getCurrentState().isClosingOrClosed();
  }

  public boolean isFollower() {
    return role.isFollower();
  }

  public boolean isCandidate() {
    return role.isCandidate();
  }

  public boolean isLeader() {
    return role.isLeader();
  }

  /**
   * return ref to the commit info cache.
   * @return commit info cache
   */
  public CommitInfoCache getCommitInfoCache() {
    return commitInfoCache;
  }

  /**
   * Change the server state to Follower if this server is in a different role or force is true.
   * @param newTerm The new term.
   * @param force Force to start a new {@link FollowerState} even if this server is already a follower.
   * @return if the term/votedFor should be updated to the new term
   */
  private synchronized boolean changeToFollower(long newTerm, boolean force, Object reason) {
    final RaftPeerRole old = role.getCurrentRole();
    final boolean metadataUpdated = state.updateCurrentTerm(newTerm);

    if (old != RaftPeerRole.FOLLOWER || force) {
      setRole(RaftPeerRole.FOLLOWER, reason);
      if (old == RaftPeerRole.LEADER) {
        role.shutdownLeaderState(false);
      } else if (old == RaftPeerRole.CANDIDATE) {
        role.shutdownLeaderElection();
      } else if (old == RaftPeerRole.FOLLOWER) {
        role.shutdownFollowerState();
      }
      role.startFollowerState(this);
    }
    return metadataUpdated;
  }

  synchronized void changeToFollowerAndPersistMetadata(long newTerm, Object reason) throws IOException {
    if (changeToFollower(newTerm, false, reason)) {
      state.persistMetadata();
    }
  }

  synchronized void changeToLeader() {
    Preconditions.assertTrue(isCandidate());
    role.shutdownLeaderElection();
    setRole(RaftPeerRole.LEADER, "changeToLeader");
    state.becomeLeader();

    // start sending AppendEntries RPC to followers
    final LogEntryProto e = role.startLeaderState(this, getProxy().getProperties());
    getState().setRaftConf(e);
  }

  Collection<CommitInfoProto> getCommitInfos() {
    final List<CommitInfoProto> infos = new ArrayList<>();
    // add the commit info of this server
    infos.add(updateCommitInfoCache());

    // add the commit infos of other servers
    if (isLeader()) {
      role.getLeaderState().ifPresent(
          leader -> leader.updateFollowerCommitInfos(commitInfoCache, infos));
    } else {
      getRaftConf().getPeers().stream()
          .map(RaftPeer::getId)
          .filter(id -> !id.equals(getId()))
          .map(commitInfoCache::get)
          .filter(Objects::nonNull)
          .forEach(infos::add);
    }
    return infos;
  }

  GroupInfoReply getGroupInfo(GroupInfoRequest request) {
    return new GroupInfoReply(request, getRoleInfoProto(),
        state.getStorage().getStorageDir().hasMetaFile(), getCommitInfos(), getGroup());
  }

  public RoleInfoProto getRoleInfoProto() {
    RaftPeerRole currentRole = role.getCurrentRole();
    RoleInfoProto.Builder roleInfo = RoleInfoProto.newBuilder()
        .setSelf(getPeer().getRaftPeerProto())
        .setRole(currentRole)
        .setRoleElapsedTimeMs(role.getRoleElapsedTimeMs());
    switch (currentRole) {
    case CANDIDATE:
      CandidateInfoProto.Builder candidate = CandidateInfoProto.newBuilder()
          .setLastLeaderElapsedTimeMs(state.getLastLeaderElapsedTimeMs());
      roleInfo.setCandidateInfo(candidate);
      break;

    case FOLLOWER:
      role.getFollowerState().ifPresent(fs -> {
        final ServerRpcProto leaderInfo = ServerProtoUtils.toServerRpcProto(
            getRaftConf().getPeer(state.getLeaderId()), fs.getLastRpcTime().elapsedTimeMs());
        roleInfo.setFollowerInfo(FollowerInfoProto.newBuilder()
            .setLeaderInfo(leaderInfo)
            .setOutstandingOp(fs.getOutstandingOp()));
      });
      break;

    case LEADER:
      role.getLeaderState().ifPresent(ls -> {
        final LeaderInfoProto.Builder leader = LeaderInfoProto.newBuilder();
        ls.getLogAppenders().map(LogAppender::getFollower).forEach(f ->
            leader.addFollowerInfo(ServerProtoUtils.toServerRpcProto(
                f.getPeer(), f.getLastRpcResponseTime().elapsedTimeMs())));
        leader.setTerm(ls.getCurrentTerm());
        roleInfo.setLeaderInfo(leader);
      });
      break;

    default:
      throw new IllegalStateException("incorrect role of server " + currentRole);
    }
    return roleInfo.build();
  }

  synchronized void changeToCandidate() {
    Preconditions.assertTrue(isFollower());
    role.shutdownFollowerState();
    setRole(RaftPeerRole.CANDIDATE, "changeToCandidate");
    if (state.shouldNotifyExtendedNoLeader()) {
      stateMachine.notifyExtendedNoLeader(getRoleInfoProto());
    }
    // start election
    role.startLeaderElection(this);
    leaderElectionMetrics.onNewLeaderElection();
  }

  @Override
  public String toString() {
    return role + " " + state + " " + lifeCycle.getCurrentState();
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(
      RaftClientRequest request, RetryCache.CacheEntry entry) {
    try {
      assertGroup(request.getRequestorId(), request.getRaftGroupId());
    } catch (GroupMismatchException e) {
      return RetryCache.failWithException(e, entry);
    }

    if (!isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = new RaftClientReply(request, exception, getCommitInfos());
      return RetryCache.failWithReply(reply, entry);
    }
    final LeaderState leaderState = role.getLeaderState().orElse(null);
    if (leaderState == null || !leaderState.isReady()) {
      RetryCache.CacheEntry cacheEntry = retryCache.get(request.getClientId(), request.getCallId());
      if (cacheEntry != null && cacheEntry.isCompletedNormally()) {
        return cacheEntry.getReplyFuture();
      }
      final LeaderNotReadyException lnre = new LeaderNotReadyException(getMemberId());
      final RaftClientReply reply = new RaftClientReply(request, lnre, getCommitInfos());
      return RetryCache.failWithReply(reply, entry);
    }
    return null;
  }

  public boolean isLeaderReady() {
    if (!isLeader()) {
      return false;
    }

    final LeaderState leaderState = role.getLeaderState().orElse(null);
    if (leaderState == null || !leaderState.isReady()) {
      return false;
    }

    return true;
  }

  NotLeaderException generateNotLeaderException() {
    if (lifeCycle.getCurrentState() != RUNNING) {
      return new NotLeaderException(getMemberId(), null, null);
    }
    RaftPeerId leaderId = state.getLeaderId();
    if (leaderId == null || leaderId.equals(getId())) {
      // No idea about who is the current leader. Or the peer is the current
      // leader, but it is about to step down. set the suggested leader as null.
      leaderId = null;
    }
    RaftConfiguration conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getPeers();
    return new NotLeaderException(getMemberId(), conf.getPeer(leaderId), peers);
  }

  private LifeCycle.State assertLifeCycleState(Set<State> expected) throws ServerNotReadyException {
    return lifeCycle.assertCurrentState((n, c) -> new ServerNotReadyException(
        getMemberId() + " is not in " + expected + ": current state is " + c),
        expected);
  }

  void assertGroup(Object requestorId, RaftGroupId requestorGroupId) throws GroupMismatchException {
    final RaftGroupId groupId = getMemberId().getGroupId();
    if (!groupId.equals(requestorGroupId)) {
      throw new GroupMismatchException(getMemberId()
          + ": The group (" + requestorGroupId + ") of " + requestorId
          + " does not match the group (" + groupId + ") of the server " + getId());
    }
  }

  /**
   * Handle a normal update request from client.
   */
  private CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TransactionContext context,
      RetryCache.CacheEntry cacheEntry) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);
    CompletableFuture<RaftClientReply> reply;

    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, cacheEntry);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final LeaderState leaderState = role.getLeaderStateNonNull();
      final PendingRequests.Permit permit = leaderState.tryAcquirePendingRequest(request.getMessage());
      if (permit == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to acquire a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      try {
        state.appendLog(context);
      } catch (StateMachineException e) {
        // the StateMachineException is thrown by the SM in the preAppend stage.
        // Return the exception in a RaftClientReply.
        RaftClientReply exceptionReply = new RaftClientReply(request, e, getCommitInfos());
        cacheEntry.failWithReply(exceptionReply);
        // leader will step down here
        if (isLeader()) {
          leaderState.submitStepDownEvent();
        }
        return CompletableFuture.completedFuture(exceptionReply);
      }

      // put the request into the pending queue
      pending = leaderState.addPendingRequest(permit, request, context);
      if (pending == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to add a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      leaderState.notifySenders();
    }
    return pending.getFuture();
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);
    LOG.debug("{}: receive client request({})", getMemberId(), request);
    Timer timer = raftServerMetrics.getClientRequestTimer(request);
    final Timer.Context timerContext = (timer != null) ? timer.time() : null;

    CompletableFuture<RaftClientReply> replyFuture;
    if (request.is(RaftClientRequestProto.TypeCase.STALEREAD)) {
      replyFuture =  staleReadAsync(request);
    } else {
      // first check the server's leader state
      CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null);
      if (reply != null) {
        return reply;
      }
      // let the state machine handle read-only request from client
      RaftClientRequest.Type type = request.getType();
      if (type.is(RaftClientRequestProto.TypeCase.STREAM)) {
        if (type.getStream().getEndOfRequest()) {
          final CompletableFuture<RaftClientRequest> f = streamEndOfRequestAsync(request);
          if (f.isCompletedExceptionally()) {
            return f.thenApply(r -> null);
          }
          request = f.join();
          type = request.getType();
        }
      }

      if (type.is(RaftClientRequestProto.TypeCase.READ)) {
        // TODO: We might not be the leader anymore by the time this completes.
        // See the RAFT paper section 8 (last part)
        replyFuture =  processQueryFuture(stateMachine.query(request.getMessage()), request);
      } else if (type.is(RaftClientRequestProto.TypeCase.WATCH)) {
        replyFuture = watchAsync(request);
      } else if (type.is(RaftClientRequestProto.TypeCase.STREAM)) {
        replyFuture = streamAsync(request);
      } else {
        // query the retry cache
        RetryCache.CacheQueryResult previousResult = retryCache.queryCache(
            request.getClientId(), request.getCallId());
        if (previousResult.isRetry()) {
          // if the previous attempt is still pending or it succeeded, return its
          // future
          replyFuture = previousResult.getEntry().getReplyFuture();
        } else {
          final RetryCache.CacheEntry cacheEntry = previousResult.getEntry();

          // TODO: this client request will not be added to pending requests until
          // later which means that any failure in between will leave partial state in
          // the state machine. We should call cancelTransaction() for failed requests
          TransactionContext context = stateMachine.startTransaction(request);
          if (context.getException() != null) {
            RaftClientReply exceptionReply = new RaftClientReply(request,
                new StateMachineException(getMemberId(), context.getException()), getCommitInfos());
            cacheEntry.failWithReply(exceptionReply);
            replyFuture =  CompletableFuture.completedFuture(exceptionReply);
          } else {
            replyFuture = appendTransaction(request, context, cacheEntry);
          }
        }
      }
    }

    replyFuture.whenComplete((clientReply, exception) -> {
      if (clientReply.isSuccess() && timerContext != null) {
        timerContext.stop();
      }
    });
    return replyFuture;
  }

  private CompletableFuture<RaftClientReply> watchAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.addWatchReqeust(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            new RaftClientReply(request, generateNotLeaderException(), getCommitInfos())));
  }

  private CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={}", getMemberId(), minIndex, commitIndex);
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          new RaftClientReply(request, new StateMachineException(getMemberId(), e), getCommitInfos()));
    }
    return processQueryFuture(stateMachine.queryStale(request.getMessage(), minIndex), request);
  }

  private CompletableFuture<RaftClientReply> streamAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.streamAsync(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            new RaftClientReply(request, generateNotLeaderException(), getCommitInfos())));
  }

  private CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.streamEndOfRequestAsync(request))
        .orElse(null);
  }

  CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> new RaftClientReply(request, r, getCommitInfos()))
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return new RaftClientReply(request, (StateMachineException)e, getCommitInfos());
          }
          throw new CompletionException(e);
        });
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(request, submitClientRequestAsync(request));
  }

  RaftClientReply waitForReply(RaftClientRequest request, CompletableFuture<RaftClientReply> future)
      throws IOException {
    return waitForReply(getMemberId(), request, future, e -> new RaftClientReply(request, e, getCommitInfos()));
  }

  static <REPLY extends RaftClientReply> REPLY waitForReply(
      Object id, RaftClientRequest request, CompletableFuture<REPLY> future,
      Function<RaftException, REPLY> exceptionReply)
      throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      final String s = id + ": Interrupted when waiting for reply, request=" + request;
      LOG.info(s, e);
      throw IOUtils.toInterruptedIOException(s, e);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause == null) {
        throw new IOException(e);
      }
      if (cause instanceof NotLeaderException ||
          cause instanceof StateMachineException) {
        final REPLY reply = exceptionReply.apply((RaftException) cause);
        if (reply != null) {
          return reply;
        }
      }
      throw IOUtils.asIOException(cause);
    }
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request) throws IOException {
    return waitForReply(request, setConfigurationAsync(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) throws IOException {
    LOG.info("{}: receive setConfiguration {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(request.getRequestorId(), request.getRaftGroupId());

    CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null);
    if (reply != null) {
      return reply;
    }

    final List<RaftPeer> peersInNewConf = request.getPeersInNewConf();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, null);
      if (reply != null) {
        return reply;
      }

      final RaftConfiguration current = getRaftConf();
      final LeaderState leaderState = role.getLeaderStateNonNull();
      // make sure there is no other raft reconfiguration in progress
      if (!current.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      // return success with a null message if the new conf is the same as the current
      if (current.hasNoChange(peersInNewConf)) {
        pending = new PendingRequest(request);
        pending.setReply(new RaftClientReply(request, getCommitInfos()));
        return pending.getFuture();
      }

      // add new peers into the rpc service
      getServerRpc().addPeers(peersInNewConf);
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request);
    }
    return pending.getFuture();
  }

  private boolean shouldWithholdVotes(long candidateTerm) {
    if (state.getCurrentTerm() < candidateTerm) {
      return false;
    } else if (isLeader()) {
      return true;
    } else {
      // following a leader and not yet timeout
      return isFollower() && state.hasLeader()
          && role.getFollowerState().map(FollowerState::shouldWithholdVotes).orElse(false);
    }
  }

  /**
   * check if the remote peer is not included in the current conf
   * and should shutdown. should shutdown if all the following stands:
   * 1. this is a leader
   * 2. current conf is stable and has been committed
   * 3. candidate id is not included in conf
   * 4. candidate's last entry's index < conf's index
   */
  private boolean shouldSendShutdown(RaftPeerId candidateId,
      TermIndex candidateLastEntry) {
    return isLeader()
        && getRaftConf().isStable()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && role.getLeaderState().map(ls -> !ls.isBootStrappingPeer(candidateId)).orElse(false);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r)
      throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        ServerProtoUtils.toTermIndex(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.debug("{}: receive requestVote({}, {}, {}, {})",
        getMemberId(), candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(candidateId, candidateGroupId);

    boolean voteGranted = false;
    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    synchronized (this) {
      final FollowerState fs = role.getFollowerState().orElse(null);
      if (shouldWithholdVotes(candidateTerm)) {
        LOG.info("{}-{}: Withhold vote from candidate {} with term {}. State: leader={}, term={}, lastRpcElapsed={}",
            getMemberId(), role, candidateId, candidateTerm, state.getLeaderId(), state.getCurrentTerm(),
            fs != null? fs.getLastRpcTime().elapsedTimeMs() + "ms": null);
      } else if (state.recognizeCandidate(candidateId, candidateTerm)) {
        final boolean termUpdated = changeToFollower(candidateTerm, true, "recognizeCandidate:" + candidateId);
        // see Section 5.4.1 Election restriction
        RaftPeer candidate = getRaftConf().getPeer(candidateId);
        if (fs != null && candidate != null) {
          int compare = ServerState.compareLog(state.getLastEntry(), candidateLastEntry);
          int priority = getRaftConf().getPeer(getId()).getPriority();
          LOG.info("{} priority:{} candidate:{} candidatePriority:{} compare:{}",
              this, priority, candidate, candidate.getPriority(), compare);
          // vote for candidate if:
          // 1. log lags behind candidate
          // 2. log equals candidate's, and priority less or equal candidate's
          if (compare < 0 || (compare == 0 && priority <= candidate.getPriority())) {
            state.grantVote(candidateId);
            voteGranted = true;
          }
        }
        if (termUpdated || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (voteGranted) {
        fs.updateLastRpcTime(FollowerState.UpdateType.REQUEST_VOTE);
      } else if(shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = ServerProtoUtils.toRequestVoteReplyProto(candidateId, getMemberId(),
          voteGranted, state.getCurrentTerm(), shouldShutdown);
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} replies to vote request: {}. Peer's state: {}",
            getMemberId(), ServerProtoUtils.toString(reply), state);
      }
    }
    return reply;
  }

  private void validateEntries(long expectedTerm, TermIndex previous,
      LogEntryProto... entries) {
    if (entries != null && entries.length > 0) {
      final long index0 = entries[0].getIndex();

      if (previous == null || previous.getTerm() == 0) {
        Preconditions.assertTrue(index0 == 0,
            "Unexpected Index: previous is null but entries[%s].getIndex()=%s",
            0, index0);
      } else {
        Preconditions.assertTrue(previous.getIndex() == index0 - 1,
            "Unexpected Index: previous is %s but entries[%s].getIndex()=%s",
            previous, 0, index0);
      }

      for (int i = 0; i < entries.length; i++) {
        final long t = entries[i].getTerm();
        Preconditions.assertTrue(expectedTerm >= t,
            "Unexpected Term: entries[%s].getTerm()=%s but expectedTerm=%s",
            i, t, expectedTerm);

        final long indexi = entries[i].getIndex();
        Preconditions.assertTrue(indexi == index0 + i,
            "Unexpected Index: entries[%s].getIndex()=%s but entries[0].getIndex()=%s",
            i, indexi, index0);
      }
    }
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    try {
      return appendEntriesAsync(r).join();
    } catch (CompletionException e) {
      throw IOUtils.asIOException(JavaUtils.unwrapCompletionException(e));
    }
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto r)
      throws IOException {
    // TODO avoid converting list to array
    final RaftRpcRequestProto request = r.getServerRequest();
    final LogEntryProto[] entries = r.getEntriesList()
        .toArray(new LogEntryProto[r.getEntriesCount()]);
    final TermIndex previous = r.hasPreviousLog() ?
        ServerProtoUtils.toTermIndex(r.getPreviousLog()) : null;
    final RaftPeerId requestorId = RaftPeerId.valueOf(request.getRequestorId());

    preAppendEntriesAsync(requestorId, ProtoUtils.toRaftGroupId(request.getRaftGroupId()), r.getLeaderTerm(),
        previous, r.getLeaderCommit(), r.getInitializing(), entries);
    try {
      return appendEntriesAsync(requestorId, r.getLeaderTerm(), previous, r.getLeaderCommit(),
          request.getCallId(), r.getInitializing(), r.getCommitInfosList(), entries);
    } catch(Throwable t) {
      LOG.error("{}: Failed appendEntriesAsync {}", getMemberId(), r, t);
      throw t;
    }
  }

  static void logAppendEntries(boolean isHeartbeat, Supplier<String> message) {
    if (isHeartbeat) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HEARTBEAT: " + message.get());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(message.get());
      }
    }
  }

  private Optional<FollowerState> updateLastRpcTime(FollowerState.UpdateType updateType) {
    final Optional<FollowerState> fs = role.getFollowerState();
    if (fs.isPresent() && lifeCycle.getCurrentState() == RUNNING) {
      fs.get().updateLastRpcTime(updateType);
      return fs;
    } else {
      return Optional.empty();
    }
  }

  private void preAppendEntriesAsync(RaftPeerId leaderId, RaftGroupId leaderGroupId, long leaderTerm,
      TermIndex previous, long leaderCommit, boolean initializing, LogEntryProto... entries) throws IOException {
    CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(),
        leaderId, leaderTerm, previous, leaderCommit, initializing, entries);

    assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
    if (!startComplete.get()) {
      throw new ServerNotReadyException(getMemberId() + ": The server role is not yet initialized.");
    }
    assertGroup(leaderId, leaderGroupId);

    try {
      validateEntries(leaderTerm, previous, entries);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
  }

  private CommitInfoProto updateCommitInfoCache() {
    return commitInfoCache.update(getPeer(), state.getLog().getLastCommittedIndex());
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      RaftPeerId leaderId, long leaderTerm, TermIndex previous, long leaderCommit, long callId, boolean initializing,
      List<CommitInfoProto> commitInfos, LogEntryProto... entries) {
    final boolean isHeartbeat = entries.length == 0;
    logAppendEntries(isHeartbeat,
        () -> getMemberId() + ": receive appendEntries(" + leaderId + ", " + leaderTerm + ", "
            + previous + ", " + leaderCommit + ", " + initializing
            + ", commits" + ProtoUtils.toString(commitInfos)
            + ", entries: " + ServerProtoUtils.toString(entries));

    final long currentTerm;
    final long followerCommit = state.getLog().getLastCommittedIndex();
    final Optional<FollowerState> followerState;
    Timer.Context timer = raftServerMetrics.getFollowerAppendEntryTimer(isHeartbeat).time();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, state.getNextIndex(), NOT_LEADER, callId,
            RaftLog.INVALID_LOG_INDEX, isHeartbeat);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Not recognize {} (term={}) as leader, state: {} reply: {}",
              getMemberId(), leaderId, leaderTerm, state, ServerProtoUtils.toString(reply));
        }
        return CompletableFuture.completedFuture(reply);
      }
      try {
        changeToFollowerAndPersistMetadata(leaderTerm, "appendEntries");
      } catch (IOException e) {
        return JavaUtils.completeExceptionally(e);
      }
      state.setLeader(leaderId, "appendEntries");

      if (!initializing && lifeCycle.compareAndTransition(STARTING, RUNNING)) {
        role.startFollowerState(this);
      }
      followerState = updateLastRpcTime(FollowerState.UpdateType.APPEND_START);

      // Check that the append entries are not inconsistent. There are 3
      // scenarios which can result in inconsistency:
      //      1. There is a snapshot installation in progress
      //      2. There is an overlap between the snapshot index and the entries
      //      3. There is a gap between the local log and the entries
      // In any of these scenarios, we should return an INCONSISTENCY reply
      // back to leader so that the leader can update this follower's next index.

      AppendEntriesReplyProto inconsistencyReply = checkInconsistentAppendEntries(
          leaderId, currentTerm, followerCommit, previous, callId, isHeartbeat, entries);
      if (inconsistencyReply != null) {
        followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
        return CompletableFuture.completedFuture(inconsistencyReply);
      }

      state.updateConfiguration(entries);
    }

    final List<CompletableFuture<Long>> futures = entries.length == 0 ? Collections.emptyList()
        : state.getLog().append(entries);
    commitInfos.forEach(commitInfoCache::update);

    if (!isHeartbeat) {
      CodeInjectionForTesting.execute(RaftLog.LOG_SYNC, getId(), null);
    }
    return JavaUtils.allOf(futures).whenCompleteAsync(
        (r, t) -> followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE))
    ).thenApply(v -> {
      final AppendEntriesReplyProto reply;
      synchronized(this) {
        state.updateStatemachine(leaderCommit, currentTerm);
        updateCommitInfoCache();
        final long n = isHeartbeat? state.getLog().getNextIndex(): entries[entries.length - 1].getIndex() + 1;
        final long matchIndex = entries.length != 0 ? entries[entries.length - 1].getIndex() :
            RaftLog.INVALID_LOG_INDEX;
        reply = ServerProtoUtils.toAppendEntriesReplyProto(leaderId, getMemberId(), currentTerm,
            state.getLog().getLastCommittedIndex(), n, SUCCESS, callId, matchIndex,
            isHeartbeat);
      }
      logAppendEntries(isHeartbeat, () ->
          getMemberId() + ": succeeded to handle AppendEntries. Reply: " + ServerProtoUtils.toString(reply));
      timer.stop();  // TODO: future never completes exceptionally?
      return reply;
    });
  }

  private AppendEntriesReplyProto checkInconsistentAppendEntries(RaftPeerId leaderId, long currentTerm,
      long followerCommit, TermIndex previous, long callId, boolean isHeartbeat, LogEntryProto... entries) {
    final long replyNextIndex = checkInconsistentAppendEntries(previous, entries);
    if (replyNextIndex == -1) {
      return null;
    }

    final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
        leaderId, getMemberId(), currentTerm, followerCommit, replyNextIndex, INCONSISTENCY, callId,
        RaftLog.INVALID_LOG_INDEX, isHeartbeat);
    LOG.info("{}: inconsistency entries. Reply:{}", getMemberId(), ServerProtoUtils.toString(reply));
    return reply;
  }

  private long checkInconsistentAppendEntries(TermIndex previous, LogEntryProto... entries) {
    // Check if a snapshot installation through state machine is in progress.
    final TermIndex installSnapshot = inProgressInstallSnapshotRequest.get();
    if (installSnapshot != null) {
      LOG.info("{}: Failed appendEntries as snapshot ({}) installation is in progress", getMemberId(), installSnapshot);
      return state.getNextIndex();
    }

    // Check that the first log entry is greater than the snapshot index in the latest snapshot.
    // If not, reply to the leader the new next index.
    if (entries != null && entries.length > 0) {
      final long firstEntryIndex = entries[0].getIndex();
      final long snapshotIndex = state.getSnapshotIndex();
      if (snapshotIndex > 0 && snapshotIndex >= firstEntryIndex) {
        LOG.info("{}: Failed appendEntries: the first entry (index {}) is already in snapshot (snapshot index: {})",
            getMemberId(), firstEntryIndex, snapshotIndex);
        return snapshotIndex + 1;
      }

      final long commitIndex =  state.getLog().getLastCommittedIndex();
      if (commitIndex > 0 && commitIndex >= firstEntryIndex) {
        LOG.info("{}: Failed appendEntries: the first entry (index {}) is already committed (commit index: {})",
            getMemberId(), firstEntryIndex, commitIndex);
        return commitIndex + 1;
      }
    }

    // Check if "previous" is contained in current state.
    if (previous != null && !state.containsTermIndex(previous)) {
      final long replyNextIndex = Math.min(state.getNextIndex(), previous.getIndex());
      LOG.info("{}: Failed appendEntries as previous log entry ({}) is not found", getMemberId(), previous);
      return replyNextIndex;
    }

    return -1;
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: receive installSnapshot: {}", getMemberId(), ServerProtoUtils.toString(request));
    }
    final InstallSnapshotReplyProto reply;
    try {
      reply = installSnapshotImpl(request);
    } catch (Throwable t) {
      LOG.error("{}: installSnapshot failed", getMemberId(), t);
      throw t;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: reply installSnapshot: {}", getMemberId(), ServerProtoUtils.toString(reply));
    }
    return reply;
  }

  private InstallSnapshotReplyProto installSnapshotImpl(InstallSnapshotRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(INSTALL_SNAPSHOT, getId(),
        leaderId, request);

    assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
    assertGroup(leaderId, leaderGroupId);

    // Check if install snapshot from Leader is enabled
    if (installSnapshotEnabled) {
      // Leader has sent InstallSnapshot request with SnapshotInfo. Install the snapshot.
      if (request.hasSnapshotChunk()) {
        return checkAndInstallSnapshot(request, leaderId);
      }
    } else {
      // Leader has only sent a notification to install snapshot. Inform State Machine to install snapshot.
      if (request.hasNotification()) {
        return notifyStateMachineToInstallSnapshot(request, leaderId);
      }
    }
    // There is a mismatch between configurations on leader and follower.
    final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(
        leaderId, getMemberId(), InstallSnapshotResult.CONF_MISMATCH);
    LOG.error("{}: Configuration Mismatch ({}): Leader {} has it set to {} but follower {} has it set to {}",
        getMemberId(), RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_KEY,
        leaderId, request.hasSnapshotChunk(), getId(), installSnapshotEnabled);
    return reply;
  }

  private InstallSnapshotReplyProto checkAndInstallSnapshot(
      InstallSnapshotRequestProto request, RaftPeerId leaderId) throws IOException {
    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest = request.getSnapshotChunk();
    final TermIndex lastTermIndex = ServerProtoUtils.toTermIndex(snapshotChunkRequest.getTermIndex());
    final long lastIncludedIndex = lastTermIndex.getIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, snapshotChunkRequest.getRequestIndex(), InstallSnapshotResult.NOT_LEADER);
        LOG.warn("{}: Failed to recognize leader for installSnapshot chunk.", getMemberId());
        return reply;
      }
      changeToFollowerAndPersistMetadata(leaderTerm, "installSnapshot");
      state.setLeader(leaderId, "installSnapshot");

      updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_START);
      try {
        // Check and append the snapshot chunk. We simply put this in lock
        // considering a follower peer requiring a snapshot installation does not
        // have a lot of requests
        Preconditions.assertTrue(
            state.getLog().getNextIndex() <= lastIncludedIndex,
            "%s log's next id is %s, last included index in snapshot is %s",
            getMemberId(), state.getLog().getNextIndex(), lastIncludedIndex);

        //TODO: We should only update State with installed snapshot once the request is done.
        state.installSnapshot(request);

        // update the committed index
        // re-load the state machine if this is the last chunk
        if (snapshotChunkRequest.getDone()) {
          state.reloadStateMachine(lastIncludedIndex);
        }
      } finally {
        updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_COMPLETE);
      }
    }
    if (snapshotChunkRequest.getDone()) {
      LOG.info("{}: successfully install the entire snapshot-{}", getMemberId(), lastIncludedIndex);
    }
    return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
        currentTerm, snapshotChunkRequest.getRequestIndex(), InstallSnapshotResult.SUCCESS);
  }

  private InstallSnapshotReplyProto notifyStateMachineToInstallSnapshot(
      InstallSnapshotRequestProto request, RaftPeerId leaderId) throws IOException {
    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    final TermIndex firstAvailableLogTermIndex = ServerProtoUtils.toTermIndex(
        request.getNotification().getFirstAvailableTermIndex());
    final long firstAvailableLogIndex = firstAvailableLogTermIndex.getIndex();

    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, InstallSnapshotResult.NOT_LEADER, -1);
        LOG.warn("{}: Failed to recognize leader for installSnapshot notification.", getMemberId());
        return reply;
      }
      changeToFollowerAndPersistMetadata(leaderTerm, "installSnapshot");
      state.setLeader(leaderId, "installSnapshot");

      updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_NOTIFICATION);

      if (inProgressInstallSnapshotRequest.compareAndSet(null, firstAvailableLogTermIndex)) {

        // Check if snapshot index is already at par or ahead of the first
        // available log index of the Leader.
        long snapshotIndex = state.getSnapshotIndex();
        if (snapshotIndex + 1 >= firstAvailableLogIndex) {
          // State Machine has already installed the snapshot. Return the
          // latest snapshot index to the Leader.

          inProgressInstallSnapshotRequest.compareAndSet(firstAvailableLogTermIndex, null);
          final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(
              leaderId, getMemberId(), currentTerm, InstallSnapshotResult.ALREADY_INSTALLED, snapshotIndex);
          LOG.info("{}: StateMachine snapshotIndex is {}", getMemberId(), snapshotIndex);
          return reply;
        }

        // This is the first installSnapshot notify request for this term and
        // index. Notify the state machine to install the snapshot.
        LOG.info("{}: notifyInstallSnapshot: nextIndex is {} but the leader's first available index is {}.",
            getMemberId(), state.getLog().getNextIndex(), firstAvailableLogIndex);

        try {
          stateMachine.notifyInstallSnapshotFromLeader(getRoleInfoProto(), firstAvailableLogTermIndex)
              .whenComplete((reply, exception) -> {
                if (exception != null) {
                  LOG.warn("{}: Failed to notify StateMachine to InstallSnapshot. Exception: {}",
                      getMemberId(), exception.getMessage());
                  inProgressInstallSnapshotRequest.compareAndSet(firstAvailableLogTermIndex, null);
                  return;
                }

                if (reply != null) {
                  LOG.info("{}: StateMachine successfully installed snapshot index {}. Reloading the StateMachine.",
                      getMemberId(), reply.getIndex());
                  stateMachine.pause();
                  state.updateInstalledSnapshotIndex(reply);
                  state.reloadStateMachine(reply.getIndex());
                }
                inProgressInstallSnapshotRequest.compareAndSet(firstAvailableLogTermIndex, null);
              });
        } catch (Throwable t) {
          inProgressInstallSnapshotRequest.compareAndSet(firstAvailableLogTermIndex, null);
          throw t;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Snapshot Installation Request received and is in progress", getMemberId());
        }
      } else {
        LOG.info("{}: Snapshot Installation by StateMachine is in progress.", getMemberId());
      }

      return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
          currentTerm, InstallSnapshotResult.IN_PROGRESS, -1);
    }
  }

  synchronized InstallSnapshotRequestProto createInstallSnapshotRequest(
      RaftPeerId targetId, String requestId, int requestIndex,
      SnapshotInfo snapshot, List<FileChunkProto> chunks, boolean done) {
    OptionalLong totalSize = snapshot.getFiles().stream()
        .mapToLong(FileInfo::getFileSize).reduce(Long::sum);
    assert totalSize.isPresent();
    return ServerProtoUtils.toInstallSnapshotRequestProto(getMemberId(), targetId,
        requestId, requestIndex, state.getCurrentTerm(), snapshot.getTermIndex(),
        chunks, totalSize.getAsLong(), done);
  }

  synchronized InstallSnapshotRequestProto createInstallSnapshotRequest(
      RaftPeerId targetId, TermIndex firstAvailableLogTermIndex) {
    assert (firstAvailableLogTermIndex.getIndex() > 0);
    return ServerProtoUtils.toInstallSnapshotRequestProto(getMemberId(), targetId,
        state.getCurrentTerm(), firstAvailableLogTermIndex);
  }

  synchronized RequestVoteRequestProto createRequestVoteRequest(
      RaftPeerId targetId, long term, TermIndex lastEntry) {
    return ServerProtoUtils.toRequestVoteRequestProto(getMemberId(), targetId, term, lastEntry);
  }

  public void submitUpdateCommitEvent() {
    role.getLeaderState().ifPresent(LeaderState::submitUpdateCommitEvent);
  }

  /**
   * The log has been submitted to the state machine. Use the future to update
   * the pending requests and retry cache.
   * @param logEntry the log entry that has been submitted to the state machine
   * @param stateMachineFuture the future returned by the state machine
   *                           from which we will get transaction result later
   */
  private CompletableFuture<Message> replyPendingRequest(
      LogEntryProto logEntry, CompletableFuture<Message> stateMachineFuture) {
    Preconditions.assertTrue(logEntry.hasStateMachineLogEntry());
    final StateMachineLogEntryProto smLog = logEntry.getStateMachineLogEntry();
    // update the retry cache
    final ClientId clientId = ClientId.valueOf(smLog.getClientId());
    final long callId = smLog.getCallId();
    final RetryCache.CacheEntry cacheEntry = retryCache.getOrCreateEntry(clientId, callId);
    if (isLeader()) {
      Preconditions.assertTrue(cacheEntry != null && !cacheEntry.isCompletedNormally(),
              "retry cache entry should be pending: %s", cacheEntry);
    }
    if (cacheEntry.isFailed()) {
      retryCache.refreshEntry(new RetryCache.CacheEntry(cacheEntry.getKey()));
    }

    final long logIndex = logEntry.getIndex();
    return stateMachineFuture.whenComplete((reply, exception) -> {
      final RaftClientReply r;
      if (exception == null) {
        r = new RaftClientReply(clientId, getMemberId(), callId, true, reply, null, logIndex, getCommitInfos());
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(getMemberId(), exception);
        r = new RaftClientReply(clientId, getMemberId(), callId, false, null, e, logIndex, getCommitInfos());
      }

      // update pending request
      synchronized (RaftServerImpl.this) {
        final LeaderState leaderState = role.getLeaderState().orElse(null);
        if (isLeader() && leaderState != null) { // is leader and is running
          leaderState.replyPendingRequest(logIndex, r);
        }
      }
      cacheEntry.updateResult(r);
    });
  }

  public long[] getFollowerNextIndices() {
    if (!isLeader()) {
      return null;
    }
    return role.getLeaderState().map(LeaderState::getFollowerNextIndices).orElse(null);
  }

  CompletableFuture<Message> applyLogToStateMachine(LogEntryProto next) {
    if (!next.hasStateMachineLogEntry()) {
      stateMachine.notifyIndexUpdate(next.getTerm(), next.getIndex());
    }
    if (next.hasConfigurationEntry()) {
      // the reply should have already been set. only need to record
      // the new conf in the metadata file.
      state.writeRaftConfiguration(next);
    } else if (next.hasStateMachineLogEntry()) {
      // check whether there is a TransactionContext because we are the leader.
      TransactionContext trx = role.getLeaderState()
          .map(leader -> leader.getTransactionContext(next.getIndex())).orElseGet(
              () -> TransactionContext.newBuilder()
                  .setServerRole(role.getCurrentRole())
                  .setStateMachine(stateMachine)
                  .setLogEntry(next)
                  .build());

      // Let the StateMachine inject logic for committed transactions in sequential order.
      trx = stateMachine.applyTransactionSerial(trx);

      try {
        // TODO: This step can be parallelized
        CompletableFuture<Message> stateMachineFuture =
            stateMachine.applyTransaction(trx);
        return replyPendingRequest(next, stateMachineFuture);
      } catch (Throwable e) {
        LOG.error("{}: applyTransaction failed for index:{} proto:{}",
            getMemberId(), next.getIndex(), ServerProtoUtils.toString(next), e);
        throw e;
      }
    }
    return null;
  }

  /**
   * The given log entry is being truncated.
   * Fail the corresponding client request, if there is any.
   *
   * @param logEntry the log entry being truncated
   */
  public void notifyTruncatedLogEntry(LogEntryProto logEntry) {
    if (logEntry.hasStateMachineLogEntry()) {
      final StateMachineLogEntryProto smLog = logEntry.getStateMachineLogEntry();
      final ClientId clientId = ClientId.valueOf(smLog.getClientId());
      final long callId = smLog.getCallId();
      final RetryCache.CacheEntry cacheEntry = getRetryCache().get(clientId, callId);
      if (cacheEntry != null) {
        final RaftClientReply reply = new RaftClientReply(clientId, getMemberId(),
            callId, false, null, generateNotLeaderException(),
            logEntry.getIndex(), getCommitInfos());
        cacheEntry.failWithReply(reply);
      }
    }
  }

  public LeaderElectionMetrics getLeaderElectionMetrics() {
    return leaderElectionMetrics;
  }

  public RaftServerMetrics getRaftServerMetrics() {
    return raftServerMetrics;
  }

  private class RaftServerJmxAdapter extends JmxRegister implements RaftServerMXBean {
    @Override
    public String getId() {
      return getMemberId().getPeerId().toString();
    }

    @Override
    public String getLeaderId() {
      return getState().getLeaderId().toString();
    }

    @Override
    public long getCurrentTerm() {
      return getState().getCurrentTerm();
    }

    @Override
    public String getGroupId() {
      return getMemberId().getGroupId().toString();
    }

    @Override
    public String getRole() {
      return role.toString();
    }

    @Override
    public List<String> getFollowers() {
      return role.getLeaderState().map(LeaderState::getFollowers).orElse(Collections.emptyList())
          .stream().map(RaftPeer::toString).collect(Collectors.toList());
    }

    @Override
    public List<String> getGroups() {
      return proxy.getGroupIds().stream().map(RaftGroupId::toString)
          .collect(Collectors.toList());
    }
  }
}
