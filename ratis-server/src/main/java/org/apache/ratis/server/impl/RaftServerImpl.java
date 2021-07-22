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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.protocol.exceptions.StaleReadException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.DivisionProperties;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.LeaderElection.Phase;
import org.apache.ratis.server.impl.RetryCacheImpl.CacheEntry;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.*;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;
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
import static org.apache.ratis.util.LifeCycle.State.EXCEPTION;
import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.PAUSED;
import static org.apache.ratis.util.LifeCycle.State.PAUSING;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

import com.codahale.metrics.Timer;

class RaftServerImpl implements RaftServer.Division,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol{
  private static final String CLASS_NAME = JavaUtils.getClassSimpleName(RaftServerImpl.class);
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";
  static final String LOG_SYNC = APPEND_ENTRIES + ".logComplete";
  static final String START_LEADER_ELECTION = CLASS_NAME + ".startLeaderElection";

  class Info implements DivisionInfo {
    @Override
    public RaftPeerRole getCurrentRole() {
      return getRole().getCurrentRole();
    }

    @Override
    public boolean isLeaderReady() {
      return isLeader() && getRole().isLeaderReady();
    }

    @Override
    public LifeCycle.State getLifeCycleState() {
      return lifeCycle.getCurrentState();
    }

    @Override
    public RoleInfoProto getRoleInfoProto() {
      return RaftServerImpl.this.getRoleInfoProto();
    }

    @Override
    public long getCurrentTerm() {
      return getState().getCurrentTerm();
    }

    @Override
    public long getLastAppliedIndex() {
      return getState().getLastAppliedIndex();
    }

    @Override
    public long[] getFollowerNextIndices() {
      return role.getLeaderState()
          .filter(leader -> isLeader())
          .map(LeaderStateImpl::getFollowerNextIndices)
          .orElse(null);
    }
  }

  private final RaftServerProxy proxy;
  private final StateMachine stateMachine;
  private final Info info =  new Info();

  private final DivisionProperties divisionProperties;
  private final int maxTimeoutMs;
  private final TimeDuration leaderStepDownWaitTime;
  private final TimeDuration sleepDeviationThreshold;
  private final boolean installSnapshotEnabled;

  private final LifeCycle lifeCycle;
  private final ServerState state;
  private final RoleInfo role;

  private final DataStreamMap dataStreamMap;

  private final MemoizedSupplier<RaftClient> raftClient;

  private final RetryCacheImpl retryCache;
  private final CommitInfoCache commitInfoCache = new CommitInfoCache();

  private final RaftServerJmxAdapter jmxAdapter;
  private final LeaderElectionMetrics leaderElectionMetrics;
  private final RaftServerMetricsImpl raftServerMetrics;

  private final AtomicReference<TermIndex> inProgressInstallSnapshotRequest;

  // To avoid append entry before complete start() method
  // For example, if thread1 start(), but before thread1 startAsFollower(), thread2 receive append entry
  // request, and change state to RUNNING by lifeCycle.compareAndTransition(STARTING, RUNNING),
  // then thread1 execute lifeCycle.transition(RUNNING) in startAsFollower(),
  // So happens IllegalStateException: ILLEGAL TRANSITION: RUNNING -> RUNNING,
  private final AtomicBoolean startComplete;

  private final TransferLeadership transferLeadership;

  RaftServerImpl(RaftGroup group, StateMachine stateMachine, RaftServerProxy proxy) throws IOException {
    final RaftPeerId id = proxy.getId();
    LOG.info("{}: new RaftServerImpl for {} with {}", id, group, stateMachine);
    this.lifeCycle = new LifeCycle(id);
    this.stateMachine = stateMachine;
    this.role = new RoleInfo(id);

    final RaftProperties properties = proxy.getProperties();
    this.divisionProperties = new DivisionPropertiesImpl(properties);
    maxTimeoutMs = properties().maxRpcTimeoutMs();
    leaderStepDownWaitTime = RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(properties);
    this.sleepDeviationThreshold = RaftServerConfigKeys.sleepDeviationThreshold(properties);
    installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(properties);
    this.proxy = proxy;

    this.state = new ServerState(id, group, properties, this, stateMachine);
    this.retryCache = new RetryCacheImpl(properties);
    this.inProgressInstallSnapshotRequest = new AtomicReference<>(null);
    this.dataStreamMap = new DataStreamMapImpl(id);

    this.jmxAdapter = new RaftServerJmxAdapter();
    this.leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(
        getMemberId(), state::getLastLeaderElapsedTimeMs);
    this.raftServerMetrics = RaftServerMetricsImpl.computeIfAbsentRaftServerMetrics(
        getMemberId(), () -> commitInfoCache::get, retryCache::getStatistics);

    this.startComplete = new AtomicBoolean(false);

    this.raftClient = JavaUtils.memoize(() -> {
      RaftClient client = RaftClient.newBuilder()
          .setRaftGroup(group)
          .setProperties(getRaftServer().getProperties())
          .build();
      return client;
    });

    this.transferLeadership = new TransferLeadership(this);
  }

  @Override
  public DivisionProperties properties() {
    return divisionProperties;
  }

  LogAppender newLogAppender(LeaderState leaderState, FollowerInfo f) {
    return getRaftServer().getFactory().newLogAppender(this, leaderState, f);
  }

  int getMaxTimeoutMs() {
    return maxTimeoutMs;
  }

  TimeDuration getRandomElectionTimeout() {
    final int min = properties().minRpcTimeoutMs();
    final int millis = min + ThreadLocalRandom.current().nextInt(properties().maxRpcTimeoutMs() - min + 1);
    return TimeDuration.valueOf(millis, TimeUnit.MILLISECONDS);
  }

  TimeDuration getLeaderStepDownWaitTime() {
    return leaderStepDownWaitTime;
  }

  TimeDuration getSleepDeviationThreshold() {
    return sleepDeviationThreshold;
  }

  @Override
  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public RaftLog getRaftLog() {
    return getState().getLog();
  }

  @Override
  public RaftStorage getRaftStorage() {
    return getState().getStorage();
  }

  @Override
  public DataStreamMap getDataStreamMap() {
    return dataStreamMap;
  }

  @Override
  public RaftClient getRaftClient() {
    return raftClient.get();
  }

  @Override
  public RetryCacheImpl getRetryCache() {
    return retryCache;
  }

  @Override
  public RaftServerProxy getRaftServer() {
    return proxy;
  }

  RaftServerRpc getServerRpc() {
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
    final RaftConfigurationImpl conf = getRaftConf();
    if (conf != null && conf.containsInBothConfs(getId())) {
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
    role.startFollowerState(this, "startAsFollower");
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

  ServerState getState() {
    return state;
  }

  @Override
  public RaftGroupMemberId getMemberId() {
    return getState().getMemberId();
  }

  @Override
  public DivisionInfo getInfo() {
    return info;
  }

  RoleInfo getRole() {
    return role;
  }

  @Override
  public RaftConfigurationImpl getRaftConf() {
    return getState().getRaftConf();
  }

  /**
   * This removes the group from the server.
   * If the deleteDirectory flag is set to false, and renameDirectory
   * the directory is moved to
   * {@link RaftServerConfigKeys#REMOVED_GROUPS_DIR_KEY} location.
   * If the deleteDirectory flag is true, the group is permanently deleted.
   */
  void groupRemove(boolean deleteDirectory, boolean renameDirectory) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();

    /* Shutdown is triggered here inorder to avoid any locked files. */
    close();
    getStateMachine().event().notifyGroupRemove();
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

  @Override
  public void close() {
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
        RaftServerMetricsImpl.removeRaftServerMetrics(getMemberId());
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to unregister metric", getMemberId(), ignored);
      }
      try {
        if (raftClient.isInitialized()) {
          raftClient.get().close();
        }
      } catch (Exception ignored) {
        LOG.warn("{}: Failed to close raft client", getMemberId(), ignored);
      }
    });
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
      role.startFollowerState(this, reason);
    }
    return metadataUpdated;
  }

  synchronized void changeToFollowerAndPersistMetadata(long newTerm, Object reason) throws IOException {
    if (changeToFollower(newTerm, false, reason)) {
      state.persistMetadata();
    }
  }

  synchronized void changeToLeader() {
    Preconditions.assertTrue(getInfo().isCandidate());
    role.shutdownLeaderElection();
    setRole(RaftPeerRole.LEADER, "changeToLeader");
    state.becomeLeader();

    // start sending AppendEntries RPC to followers
    final LogEntryProto e = role.startLeaderState(this);
    getState().setRaftConf(e);
  }

  Collection<CommitInfoProto> getCommitInfos() {
    final List<CommitInfoProto> infos = new ArrayList<>();
    // add the commit info of this server
    infos.add(updateCommitInfoCache());

    // add the commit infos of other servers
    if (getInfo().isLeader()) {
      role.getLeaderState().ifPresent(
          leader -> leader.updateFollowerCommitInfos(commitInfoCache, infos));
    } else {
      getRaftConf().getAllPeers().stream()
          .map(RaftPeer::getId)
          .filter(id -> !id.equals(getId()))
          .map(commitInfoCache::get)
          .filter(Objects::nonNull)
          .forEach(infos::add);
    }
    return infos;
  }

  GroupInfoReply getGroupInfo(GroupInfoRequest request) {
    return new GroupInfoReply(request, getCommitInfos(),
        getGroup(), getRoleInfoProto(), state.getStorage().getStorageDir().isHealthy());
  }

  private RoleInfoProto getRoleInfoProto(RaftPeer leaderPeerInfo) {
    RaftPeerRole currentRole = role.getCurrentRole();
    RoleInfoProto.Builder roleInfo = RoleInfoProto.newBuilder()
        .setSelf(getPeer().getRaftPeerProto())
        .setRole(currentRole)
        .setRoleElapsedTimeMs(role.getRoleElapsedTimeMs());
    final Optional<FollowerState> fs = role.getFollowerState();
    final ServerRpcProto leaderInfo =
        ServerProtoUtils.toServerRpcProto(leaderPeerInfo,
            fs.map(FollowerState::getLastRpcTime).map(Timestamp::elapsedTimeMs).orElse(0L));
    roleInfo.setFollowerInfo(FollowerInfoProto.newBuilder().setLeaderInfo(leaderInfo)
        .setOutstandingOp(fs.map(FollowerState::getOutstandingOp).orElse(0)));
    return roleInfo.build();
  }

  RoleInfoProto getRoleInfoProto() {
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
      final Optional<FollowerState> fs = role.getFollowerState();
      final ServerRpcProto leaderInfo = ServerProtoUtils.toServerRpcProto(
        getRaftConf().getPeer(state.getLeaderId()),
        fs.map(FollowerState::getLastRpcTime).map(Timestamp::elapsedTimeMs).orElse(0L));
      // FollowerState can be null while adding a new peer as it is not
      // a voting member yet
      roleInfo.setFollowerInfo(FollowerInfoProto.newBuilder()
        .setLeaderInfo(leaderInfo)
        .setOutstandingOp(fs.map(FollowerState::getOutstandingOp).orElse(0)));
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

  synchronized void changeToCandidate(boolean forceStartLeaderElection) {
    Preconditions.assertTrue(getInfo().isFollower());
    role.shutdownFollowerState();
    setRole(RaftPeerRole.CANDIDATE, "changeToCandidate");
    if (state.shouldNotifyExtendedNoLeader()) {
      stateMachine.followerEvent().notifyExtendedNoLeader(getRoleInfoProto());
    }
    // start election
    role.startLeaderElection(this, forceStartLeaderElection);
  }

  @Override
  public String toString() {
    return role + " " + state + " " + lifeCycle.getCurrentState();
  }

  RaftClientReply.Builder newReplyBuilder(RaftClientRequest request) {
    return RaftClientReply.newBuilder()
        .setRequest(request)
        .setCommitInfos(getCommitInfos());
  }

  private RaftClientReply.Builder newReplyBuilder(ClientInvocationId invocationId, long logIndex) {
    return RaftClientReply.newBuilder()
        .setClientInvocationId(invocationId)
        .setLogIndex(logIndex)
        .setServerId(getMemberId())
        .setCommitInfos(getCommitInfos());
  }

  RaftClientReply newSuccessReply(RaftClientRequest request) {
    return newReplyBuilder(request)
        .setSuccess()
        .build();
  }

  RaftClientReply newExceptionReply(RaftClientRequest request, RaftException exception) {
    return newReplyBuilder(request)
        .setException(exception)
        .build();
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(RaftClientRequest request, CacheEntry entry,
      boolean isWrite) {
    try {
      assertGroup(request.getRequestorId(), request.getRaftGroupId());
    } catch (GroupMismatchException e) {
      return RetryCacheImpl.failWithException(e, entry);
    }

    if (!getInfo().isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = newExceptionReply(request, exception);
      return RetryCacheImpl.failWithReply(reply, entry);
    }
    if (!getInfo().isLeaderReady()) {
      final CacheEntry cacheEntry = retryCache.getIfPresent(ClientInvocationId.valueOf(request));
      if (cacheEntry != null && cacheEntry.isCompletedNormally()) {
        return cacheEntry.getReplyFuture();
      }
      final LeaderNotReadyException lnre = new LeaderNotReadyException(getMemberId());
      final RaftClientReply reply = newExceptionReply(request, lnre);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    if (isWrite && isSteppingDown()) {
      final LeaderSteppingDownException lsde = new LeaderSteppingDownException(getMemberId() + " is stepping down");
      final RaftClientReply reply = newExceptionReply(request, lsde);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    return null;
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
    final RaftConfigurationImpl conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getAllPeers();
    return new NotLeaderException(getMemberId(), conf.getPeer(leaderId), peers);
  }

  private LifeCycle.State assertLifeCycleState(Set<LifeCycle.State> expected) throws ServerNotReadyException {
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
      RaftClientRequest request, TransactionContext context, CacheEntry cacheEntry) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);
    CompletableFuture<RaftClientReply> reply;

    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, cacheEntry, true);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
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
        RaftClientReply exceptionReply = newExceptionReply(request, e);
        cacheEntry.failWithReply(exceptionReply);
        // leader will step down here
        if (e.leaderShouldStepDown() && getInfo().isLeader()) {
          leaderState.submitStepDownEvent(LeaderState.StepDownReason.STATE_MACHINE_EXCEPTION);
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

  void stepDownOnJvmPause() {
    if (getInfo().isLeader()) {
      role.getLeaderState().ifPresent(leader -> leader.submitStepDownEvent(LeaderState.StepDownReason.JVM_PAUSE));
    }
  }

  private RaftClientRequest filterDataStreamRaftClientRequest(RaftClientRequest request)
      throws InvalidProtocolBufferException {
    return !request.is(TypeCase.FORWARD) ? request : ClientProtoUtils.toRaftClientRequest(
        RaftClientRequestProto.parseFrom(
            request.getMessage().getContent().asReadOnlyByteBuffer()));
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);
    LOG.debug("{}: receive client request({})", getMemberId(), request);
    final Optional<Timer> timer = Optional.ofNullable(raftServerMetrics.getClientRequestTimer(request.getType()));

    final CompletableFuture<RaftClientReply> replyFuture;

    if (request.is(TypeCase.STALEREAD)) {
      replyFuture = staleReadAsync(request);
    } else {
      // first check the server's leader state
      CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null,
          !request.is(TypeCase.READ) && !request.is(TypeCase.WATCH));
      if (reply != null) {
        return reply;
      }

      // let the state machine handle read-only request from client
      RaftClientRequest.Type type = request.getType();
      if (type.is(TypeCase.MESSAGESTREAM)) {
        if (type.getMessageStream().getEndOfRequest()) {
          final CompletableFuture<RaftClientRequest> f = streamEndOfRequestAsync(request);
          if (f.isCompletedExceptionally()) {
            return f.thenApply(r -> null);
          }
          request = f.join();
          type = request.getType();
        }
      }

      if (type.is(TypeCase.READ)) {
        // TODO: We might not be the leader anymore by the time this completes.
        // See the RAFT paper section 8 (last part)
        replyFuture = processQueryFuture(stateMachine.query(request.getMessage()), request);
      } else if (type.is(TypeCase.WATCH)) {
        replyFuture = watchAsync(request);
      } else if (type.is(TypeCase.MESSAGESTREAM)) {
        replyFuture = streamAsync(request);
      } else {
        // query the retry cache
        final RetryCacheImpl.CacheQueryResult queryResult = retryCache.queryCache(ClientInvocationId.valueOf(request));
        final CacheEntry cacheEntry = queryResult.getEntry();
        if (queryResult.isRetry()) {
          // if the previous attempt is still pending or it succeeded, return its
          // future
          replyFuture = cacheEntry.getReplyFuture();
        } else {
          // TODO: this client request will not be added to pending requests until
          // later which means that any failure in between will leave partial state in
          // the state machine. We should call cancelTransaction() for failed requests
          TransactionContext context = stateMachine.startTransaction(filterDataStreamRaftClientRequest(request));
          if (context.getException() != null) {
            final StateMachineException e = new StateMachineException(getMemberId(), context.getException());
            final RaftClientReply exceptionReply = newExceptionReply(request, e);
            cacheEntry.failWithReply(exceptionReply);
            replyFuture =  CompletableFuture.completedFuture(exceptionReply);
          } else {
            replyFuture = appendTransaction(request, context, cacheEntry);
          }
        }
      }
    }

    final RaftClientRequest.Type type = request.getType();
    replyFuture.whenComplete((clientReply, exception) -> {
      if (clientReply.isSuccess()) {
        timer.map(Timer::time).ifPresent(Timer.Context::stop);
      }
      if (exception != null || clientReply.getException() != null) {
        raftServerMetrics.incFailedRequestCount(type);
      }
    });
    return replyFuture;
  }

  private CompletableFuture<RaftClientReply> watchAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.addWatchReqeust(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  private CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={}", getMemberId(), minIndex, commitIndex);
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          newExceptionReply(request, new StateMachineException(getMemberId(), e)));
    }
    return processQueryFuture(stateMachine.queryStale(request.getMessage(), minIndex), request);
  }

  private CompletableFuture<RaftClientReply> streamAsync(RaftClientRequest request) {
    return role.getLeaderState()
        .map(ls -> ls.streamAsync(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftClientRequest request) {
    return role.getLeaderState()
            .map(ls -> ls.streamEndOfRequestAsync(request))
            .orElseGet(() -> {
              final CompletableFuture<RaftClientRequest> errorF = new CompletableFuture<>();
              errorF.completeExceptionally(
                      new Exception("Unexpected null encountered, while receiving end of stream request."));
              return errorF;
            });
  }

  CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> newReplyBuilder(request).setSuccess().setMessage(r).build())
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return newExceptionReply(request, (StateMachineException)e);
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
    return waitForReply(getMemberId(), request, future, e -> newExceptionReply(request, e));
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
      Thread.currentThread().interrupt();
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

  public RaftClientReply transferLeadership(TransferLeadershipRequest request) throws IOException {
    return waitForReply(request, transferLeadershipAsync(request));
  }

  private CompletableFuture<RaftClientReply> logAndReturnTransferLeadershipFail(
      TransferLeadershipRequest request, String msg) {
    LOG.warn(msg);
    return CompletableFuture.completedFuture(
        newExceptionReply(request, new TransferLeadershipException(msg)));
  }

  boolean isSteppingDown() {
    return transferLeadership.isSteppingDown();
  }

  void finishTransferLeadership() {
    transferLeadership.finish(state.getLeaderId(), false);
  }

  public CompletableFuture<RaftClientReply> transferLeadershipAsync(TransferLeadershipRequest request)
      throws IOException {
    LOG.info("{}: receive transferLeadership {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(request.getRequestorId(), request.getRaftGroupId());

    synchronized (this) {
      CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null, false);
      if (reply != null) {
        return reply;
      }

      if (getId().equals(request.getNewLeader())) {
        return CompletableFuture.completedFuture(newSuccessReply(request));
      }

      final RaftConfigurationImpl conf = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();

      // make sure there is no raft reconfiguration in progress
      if (!conf.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " when raft reconfiguration in progress.";
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.containsInConf(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it is not in " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.isHighestPriority(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it does not has highest priority " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      return transferLeadership.start(request);
    }
  }

  public RaftClientReply setConfiguration(SetConfigurationRequest request) throws IOException {
    return waitForReply(request, setConfigurationAsync(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) throws IOException {
    LOG.info("{}: receive setConfiguration {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(request.getRequestorId(), request.getRaftGroupId());

    CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null, true);
    if (reply != null) {
      return reply;
    }

    final List<RaftPeer> peersInNewConf = request.getPeersInNewConf();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, null, false);
      if (reply != null) {
        return reply;
      }

      final RaftConfigurationImpl current = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
      // make sure there is no other raft reconfiguration in progress
      if (!current.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      // return success with a null message if the new conf is the same as the current
      if (current.hasNoChange(peersInNewConf)) {
        pending = new PendingRequest(request);
        pending.setReply(newSuccessReply(request));
        return pending.getFuture();
      }

      getRaftServer().addRaftPeers(peersInNewConf);
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request);
    }
    return pending.getFuture();
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
    return getInfo().isLeader()
        && getRaftConf().isStable()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && role.getLeaderState().map(ls -> !ls.isBootStrappingPeer(candidateId)).orElse(false);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(r.getPreVote() ? Phase.PRE_VOTE : Phase.ELECTION,
        RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        TermIndex.valueOf(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(Phase phase,
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.info("{}: receive requestVote({}, {}, {}, {}, {})",
        getMemberId(), phase, candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(candidateId, candidateGroupId);

    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.RUNNING);

      final VoteContext context = new VoteContext(this, phase, candidateId);
      final RaftPeer candidate = context.recognizeCandidate(candidateTerm);
      final boolean voteGranted = context.decideVote(candidate, candidateLastEntry);
      if (candidate != null && phase == Phase.ELECTION) {
        // change server state in the ELECTION phase
        final boolean termUpdated = changeToFollower(candidateTerm, true, "candidate:" + candidateId);
        if (voteGranted) {
          state.grantVote(candidate.getId());
        }
        if (termUpdated || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (voteGranted) {
        role.getFollowerState().ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.REQUEST_VOTE));
      } else if(shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = ServerProtoUtils.toRequestVoteReplyProto(candidateId, getMemberId(),
          voteGranted, state.getCurrentTerm(), shouldShutdown);
      if (LOG.isInfoEnabled()) {
        LOG.info("{} replies to {} vote request: {}. Peer's state: {}",
            getMemberId(), phase, ServerStringUtils.toRequestVoteReplyString(reply), state);
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
    final TermIndex previous = r.hasPreviousLog()? TermIndex.valueOf(r.getPreviousLog()) : null;
    final RaftPeerId requestorId = RaftPeerId.valueOf(request.getRequestorId());

    preAppendEntriesAsync(requestorId, ProtoUtils.toRaftGroupId(request.getRaftGroupId()), r.getLeaderTerm(),
        previous, r.getLeaderCommit(), r.getInitializing(), entries);
    try {
      return appendEntriesAsync(requestorId, r.getLeaderTerm(), previous, r.getLeaderCommit(),
          request.getCallId(), r.getInitializing(), r.getCommitInfosList(), entries);
    } catch(Exception t) {
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
      List<CommitInfoProto> commitInfos, LogEntryProto... entries) throws IOException {
    final boolean isHeartbeat = entries.length == 0;
    logAppendEntries(isHeartbeat,
        () -> getMemberId() + ": receive appendEntries(" + leaderId + ", " + leaderTerm + ", "
            + previous + ", " + leaderCommit + ", " + initializing
            + ", commits" + ProtoUtils.toString(commitInfos)
            + ", entries: " + LogProtoUtils.toLogEntriesString(entries));

    final long currentTerm;
    final long followerCommit = state.getLog().getLastCommittedIndex();
    final Optional<FollowerState> followerState;
    Timer.Context timer = raftServerMetrics.getFollowerAppendEntryTimer(isHeartbeat).time();
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, state.getNextIndex(), NOT_LEADER, callId,
            RaftLog.INVALID_LOG_INDEX, isHeartbeat);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Not recognize {} (term={}) as leader, state: {} reply: {}",
              getMemberId(), leaderId, leaderTerm, state, ServerStringUtils.toAppendEntriesReplyString(reply));
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
        role.startFollowerState(this, Op.APPEND_ENTRIES);
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
      CodeInjectionForTesting.execute(LOG_SYNC, getId(), null);
    }
    return JavaUtils.allOf(futures).whenCompleteAsync(
        (r, t) -> followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE))
    ).thenApply(v -> {
      final AppendEntriesReplyProto reply;
      synchronized(this) {
        final long commitIndex = ServerImplUtils.effectiveCommitIndex(leaderCommit, previous, entries.length);
        state.updateCommitIndex(commitIndex, currentTerm, false);
        updateCommitInfoCache();
        final long n = isHeartbeat? state.getLog().getNextIndex(): entries[entries.length - 1].getIndex() + 1;
        final long matchIndex = entries.length != 0 ? entries[entries.length - 1].getIndex() :
            RaftLog.INVALID_LOG_INDEX;
        reply = ServerProtoUtils.toAppendEntriesReplyProto(leaderId, getMemberId(), currentTerm,
            state.getLog().getLastCommittedIndex(), n, SUCCESS, callId, matchIndex,
            isHeartbeat);
      }
      logAppendEntries(isHeartbeat, () -> getMemberId() + ": succeeded to handle AppendEntries. Reply: "
          + ServerStringUtils.toAppendEntriesReplyString(reply));
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
    LOG.info("{}: inconsistency entries. Reply:{}", getMemberId(), ServerStringUtils.toAppendEntriesReplyString(reply));
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
      LOG.info("{}: receive installSnapshot: {}", getMemberId(),
          ServerStringUtils.toInstallSnapshotRequestString(request));
    }
    final InstallSnapshotReplyProto reply;
    try {
      reply = installSnapshotImpl(request);
    } catch (Exception e) {
      LOG.error("{}: installSnapshot failed", getMemberId(), e);
      throw e;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: reply installSnapshot: {}", getMemberId(),
          ServerStringUtils.toInstallSnapshotReplyString(reply));
    }
    return reply;
  }

  boolean pause() throws IOException {
    // TODO: should pause() be limited on only working for a follower?

    // Now the state of lifeCycle should be PAUSING, which will prevent future other operations.
    // Pause() should pause ongoing operations:
    //  a. call {@link StateMachine#pause()}.
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(RUNNING, PAUSING)) {
        return false;
      }
      // TODO: any other operations that needs to be paused?
      stateMachine.pause();
      lifeCycle.compareAndTransition(PAUSING, PAUSED);
    }
    return true;
  }

  boolean resume() throws IOException {
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(PAUSED, STARTING)) {
        return false;
      }
      // TODO: any other operations that needs to be resumed?
      try {
        stateMachine.reinitialize();
      } catch (IOException e) {
        LOG.warn("Failed to reinitialize statemachine: {}", stateMachine.toString());
        lifeCycle.compareAndTransition(STARTING, EXCEPTION);
        throw e;
      }
      lifeCycle.compareAndTransition(STARTING, RUNNING);
    }
    return true;
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    final TermIndex leaderLastEntry = TermIndex.valueOf(request.getLeaderLastEntry());

    CodeInjectionForTesting.execute(START_LEADER_ELECTION, getId(), leaderId, request);

    LOG.debug("{}: receive startLeaderElection from:{}, leaderLastEntry:{},",
        getMemberId(), leaderId, request.getLeaderLastEntry());

    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(leaderId, leaderGroupId);

    synchronized (this) {
      // leaderLastEntry should not be null because LeaderStateImpl#start append a placeHolder entry
      // so leader at each term should has at least one entry
      if (leaderLastEntry == null) {
        LOG.warn("{}: receive null leaderLastEntry which is unexpected", getMemberId());
        return ServerProtoUtils.toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      final boolean recognized = state.recognizeLeader(leaderId, leaderLastEntry.getTerm());
      if (!recognized) {
        LOG.warn("{}: Not recognize {} (term={}) as leader, state: {}",
            getMemberId(), leaderId, leaderLastEntry.getTerm(), state);
        return ServerProtoUtils.toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (!getInfo().isFollower()) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because role is:{}",
            getMemberId(), leaderId, role.getCurrentRole());
        return ServerProtoUtils.toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (ServerState.compareLog(state.getLastEntry(), leaderLastEntry) < 0) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because lastEntry:{} less than leaderEntry:{}",
            getMemberId(), leaderId, leaderLastEntry, state.getLastEntry());
        return ServerProtoUtils.toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      changeToCandidate(true);
      return ServerProtoUtils.toStartLeaderElectionReplyProto(leaderId, getMemberId(), true);
    }
  }

  private InstallSnapshotReplyProto installSnapshotImpl(InstallSnapshotRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(INSTALL_SNAPSHOT, getId(),
        leaderId, request);

    assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
    assertGroup(leaderId, leaderGroupId);

    InstallSnapshotReplyProto reply = null;
    // Check if install snapshot from Leader is enabled
    if (installSnapshotEnabled) {
      // Leader has sent InstallSnapshot request with SnapshotInfo. Install the snapshot.
      if (request.hasSnapshotChunk()) {
        reply = checkAndInstallSnapshot(request, leaderId);
      }
    } else {
      // Leader has only sent a notification to install snapshot. Inform State Machine to install snapshot.
      if (request.hasNotification()) {
        reply = notifyStateMachineToInstallSnapshot(request, leaderId);
      }
    }

    if (reply != null) {
      if (request.hasLastRaftConfigurationLogEntryProto()) {
        // Set the configuration included in the snapshot
        LogEntryProto newConfLogEntryProto =
            request.getLastRaftConfigurationLogEntryProto();
        LOG.info("{}: set new configuration {} from snapshot", getMemberId(),
            newConfLogEntryProto);

        state.setRaftConf(newConfLogEntryProto);
        state.writeRaftConfiguration(newConfLogEntryProto);
        stateMachine.event().notifyConfigurationChanged(newConfLogEntryProto.getTerm(), newConfLogEntryProto.getIndex(),
            newConfLogEntryProto.getConfigurationEntry());
      }
      return reply;
    }

    // There is a mismatch between configurations on leader and follower.
    final InstallSnapshotReplyProto failedReply = ServerProtoUtils.toInstallSnapshotReplyProto(
        leaderId, getMemberId(), InstallSnapshotResult.CONF_MISMATCH);
    LOG.error("{}: Configuration Mismatch ({}): Leader {} has it set to {} but follower {} has it set to {}",
        getMemberId(), RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_KEY,
        leaderId, request.hasSnapshotChunk(), getId(), installSnapshotEnabled);
    return failedReply;
  }

  private InstallSnapshotReplyProto checkAndInstallSnapshot(
      InstallSnapshotRequestProto request, RaftPeerId leaderId) throws IOException {
    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest = request.getSnapshotChunk();
    final long lastIncludedIndex = snapshotChunkRequest.getTermIndex().getIndex();
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
    final TermIndex firstAvailableLogTermIndex = TermIndex.valueOf(
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

        RaftPeerProto leaderPeerInfo = null;
        if (request.hasLastRaftConfigurationLogEntryProto()) {
          List<RaftPeerProto> peerList = request.getLastRaftConfigurationLogEntryProto().getConfigurationEntry()
              .getPeersList();
          Optional<RaftPeerProto> optionalLeaderPeerInfo = peerList.stream()
                  .filter(p -> RaftPeerId.valueOf(p.getId()).equals(leaderId)).findFirst();
          leaderPeerInfo = (optionalLeaderPeerInfo.isPresent()) ? optionalLeaderPeerInfo.get() : null;
        }

        // For the cases where RaftConf is empty on newly started peer with
        // empty peer list, we retrieve leader info from
        // installSnapShotRequestProto.
        RoleInfoProto roleInfoProto;
        RaftPeer raftPeer = getRaftConf().getPeer(state.getLeaderId());
        if (raftPeer == null) {
          if (leaderPeerInfo != null) {
            roleInfoProto = getRoleInfoProto(ProtoUtils.toRaftPeer(leaderPeerInfo));
          } else {
            throw new IOException("Leader peer info is unknown.");
          }
        } else {
          roleInfoProto = getRoleInfoProto();
        }

        // This is the first installSnapshot notify request for this term and
        // index. Notify the state machine to install the snapshot.
        LOG.info("{}: notifyInstallSnapshot: nextIndex is {} but the leader's first available index is {}.",
            getMemberId(), state.getLog().getNextIndex(), firstAvailableLogIndex);
        try {
          stateMachine.followerEvent().notifyInstallSnapshotFromLeader(roleInfoProto, firstAvailableLogTermIndex)
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

  void submitUpdateCommitEvent() {
    role.getLeaderState().ifPresent(LeaderStateImpl::submitUpdateCommitEvent);
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
    final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
    // update the retry cache
    final CacheEntry cacheEntry = retryCache.getOrCreateEntry(invocationId);
    if (getInfo().isLeader()) {
      Preconditions.assertTrue(!cacheEntry.isCompletedNormally(),
              "retry cache entry should be pending: %s", cacheEntry);
    }
    if (cacheEntry.isFailed()) {
      retryCache.refreshEntry(new CacheEntry(cacheEntry.getKey()));
    }

    final long logIndex = logEntry.getIndex();
    return stateMachineFuture.whenComplete((reply, exception) -> {
      final RaftClientReply.Builder b = newReplyBuilder(invocationId, logIndex);
      final RaftClientReply r;
      if (exception == null) {
        r = b.setSuccess().setMessage(reply).build();
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(getMemberId(), exception);
        r = b.setException(e).build();
      }

      // update pending request
      synchronized (RaftServerImpl.this) {
        if (getInfo().isLeader()) {
          role.getLeaderState().ifPresent(leader -> leader.replyPendingRequest(logIndex, r));
        }
      }
      cacheEntry.updateResult(r);
    });
  }

  CompletableFuture<Message> applyLogToStateMachine(LogEntryProto next) throws RaftLogIOException {
    if (!next.hasStateMachineLogEntry()) {
      stateMachine.event().notifyTermIndexUpdated(next.getTerm(), next.getIndex());
    }

    if (next.hasConfigurationEntry()) {
      // the reply should have already been set. only need to record
      // the new conf in the metadata file and notify the StateMachine.
      state.writeRaftConfiguration(next);
      stateMachine.event().notifyConfigurationChanged(next.getTerm(), next.getIndex(), next.getConfigurationEntry());
    } else if (next.hasStateMachineLogEntry()) {
      // check whether there is a TransactionContext because we are the leader.
      TransactionContext trx = role.getLeaderState()
          .map(leader -> leader.getTransactionContext(next.getIndex())).orElseGet(
              () -> TransactionContext.newBuilder()
                  .setServerRole(role.getCurrentRole())
                  .setStateMachine(stateMachine)
                  .setLogEntry(next)
                  .build());

      try {
        // Let the StateMachine inject logic for committed transactions in sequential order.
        trx = stateMachine.applyTransactionSerial(trx);

        final CompletableFuture<Message> stateMachineFuture = stateMachine.applyTransaction(trx);
        return replyPendingRequest(next, stateMachineFuture);
      } catch (Exception e) {
        throw new RaftLogIOException(e);
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
  void notifyTruncatedLogEntry(LogEntryProto logEntry) {
    if (logEntry.hasStateMachineLogEntry()) {
      final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
      final CacheEntry cacheEntry = getRetryCache().getIfPresent(invocationId);
      if (cacheEntry != null) {
        cacheEntry.failWithReply(newReplyBuilder(invocationId, logEntry.getIndex())
            .setException(generateNotLeaderException())
            .build());
      }
    }
  }

  LeaderElectionMetrics getLeaderElectionMetrics() {
    return leaderElectionMetrics;
  }

  @Override
  public RaftServerMetricsImpl getRaftServerMetrics() {
    return raftServerMetrics;
  }

  private class RaftServerJmxAdapter extends JmxRegister implements RaftServerMXBean {
    @Override
    public String getId() {
      return getMemberId().getPeerId().toString();
    }

    @Override
    public String getLeaderId() {
      RaftPeerId leaderId = getState().getLeaderId();
      if (leaderId != null) {
        return leaderId.toString();
      } else {
        return null;
      }
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
      return role.getLeaderState().map(LeaderStateImpl::getFollowers).orElse(Collections.emptyList())
          .stream().map(RaftPeer::toString).collect(Collectors.toList());
    }

    @Override
    public List<String> getGroups() {
      return proxy.getGroupIds().stream().map(RaftGroupId::toString)
          .collect(Collectors.toList());
    }
  }
}
