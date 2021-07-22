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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.ReconfigurationTimeoutException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.metrics.LogAppenderMetrics;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.server.RaftServer.Division.LOG;

/**
 * States for leader only. It contains three different types of processors:
 * 1. RPC senders: each thread is appending log to a follower
 * 2. EventProcessor: a single thread updating the raft server's state based on
 *                    status of log appending response
 * 3. PendingRequestHandler: a handler sending back responses to clients when
 *                           corresponding log entries are committed
 */
class LeaderStateImpl implements LeaderState {
  public static final String APPEND_PLACEHOLDER = JavaUtils.getClassSimpleName(LeaderState.class) + ".placeholder";

  private enum BootStrapProgress {
    NOPROGRESS, PROGRESSING, CAUGHTUP
  }

  static class StateUpdateEvent {
    private enum Type {
      STEP_DOWN, UPDATE_COMMIT, CHECK_STAGING
    }

    private final Type type;
    private final long newTerm;
    private final Runnable handler;

    StateUpdateEvent(Type type, long newTerm, Runnable handler) {
      this.type = type;
      this.newTerm = newTerm;
      this.handler = handler;
    }

    void execute() {
      handler.run();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof StateUpdateEvent)) {
        return false;
      }
      final StateUpdateEvent that = (StateUpdateEvent)obj;
      return this.type == that.type && this.newTerm == that.newTerm;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, newTerm);
    }

    @Override
    public String toString() {
      return type + (newTerm >= 0? ":" + newTerm: "");
    }
  }

  private class EventQueue {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final BlockingQueue<StateUpdateEvent> queue = new ArrayBlockingQueue<>(4096);

    void submit(StateUpdateEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        LOG.info("{}: Interrupted when submitting {} ", this, event);
        Thread.currentThread().interrupt();
      }
    }

    StateUpdateEvent poll() {
      final StateUpdateEvent e;
      try {
        e = queue.poll(server.getMaxTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        String s = this + ": poll() is interrupted";
        if (!running) {
          LOG.info(s + " gracefully");
          return null;
        } else {
          throw new IllegalStateException(s + " UNEXPECTEDLY", ie);
        }
      }

      if (e != null) {
        // remove duplicated events from the head.
        while(e.equals(queue.peek())) {
          queue.poll();
        }
      }
      return e;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Use {@link CopyOnWriteArrayList} to implement a thread-safe list.
   * Since each mutation induces a copy of the list, only bulk operations
   * (addAll and removeAll) are supported.
   */
  static class SenderList {
    private final List<LogAppender> senders;

    SenderList() {
      this.senders = new CopyOnWriteArrayList<>();
    }

    Stream<LogAppender> stream() {
      return senders.stream();
    }

    List<LogAppender> getSenders() {
      return senders;
    }

    void forEach(Consumer<LogAppender> action) {
      senders.forEach(action);
    }

    void addAll(Collection<LogAppender> newSenders) {
      if (newSenders.isEmpty()) {
        return;
      }

      Preconditions.assertUnique(
          CollectionUtils.as(senders, LogAppender::getFollowerId),
          CollectionUtils.as(newSenders, LogAppender::getFollowerId));

      final boolean changed = senders.addAll(newSenders);
      Preconditions.assertTrue(changed);
    }

    boolean removeAll(Collection<LogAppender> c) {
      return senders.removeAll(c);
    }
  }

  private final StateUpdateEvent updateCommitEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.UPDATE_COMMIT, -1, this::updateCommit);
  private final StateUpdateEvent checkStagingEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.CHECK_STAGING, -1, this::checkStaging);

  private final String name;
  private final RaftServerImpl server;
  private final RaftLog raftLog;
  private final long currentTerm;
  private volatile ConfigurationStagingState stagingState;
  private List<List<RaftPeerId>> voterLists;
  private final Map<RaftPeerId, FollowerInfo> peerIdFollowerInfoMap = new ConcurrentHashMap<>();

  /**
   * The list of threads appending entries to followers.
   * The list is protected by the RaftServer's lock.
   */
  private final SenderList senders;
  private final EventQueue eventQueue;
  private final EventProcessor processor;
  private final PendingRequests pendingRequests;
  private final WatchRequests watchRequests;
  private final MessageStreamRequests messageStreamRequests;
  private volatile boolean running = true;

  private final int stagingCatchupGap;
  private final long placeHolderIndex;
  private final RaftServerMetricsImpl raftServerMetrics;
  private final LogAppenderMetrics logAppenderMetrics;

  LeaderStateImpl(RaftServerImpl server) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;

    final RaftProperties properties = server.getRaftServer().getProperties();
    stagingCatchupGap = RaftServerConfigKeys.stagingCatchupGap(properties);

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();

    this.eventQueue = new EventQueue();
    processor = new EventProcessor(this.name);
    raftServerMetrics = server.getRaftServerMetrics();
    logAppenderMetrics = new LogAppenderMetrics(server.getMemberId());
    this.pendingRequests = new PendingRequests(server.getMemberId(), properties, raftServerMetrics);
    this.watchRequests = new WatchRequests(server.getMemberId(), properties);
    this.messageStreamRequests = new MessageStreamRequests(server.getMemberId());

    final RaftConfigurationImpl conf = state.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(server.getId());
    placeHolderIndex = raftLog.getNextIndex();

    senders = new SenderList();
    addSenders(others, placeHolderIndex, true);
    voterLists = divideFollowers(conf);
  }

  LogEntryProto start() {
    // In the beginning of the new term, replicate a conf entry in order
    // to finally commit entries in the previous term.
    // Also this message can help identify the last committed index and the conf.
    final LogEntryProto placeHolder = LogProtoUtils.toLogEntryProto(
        server.getRaftConf(), server.getState().getCurrentTerm(), raftLog.getNextIndex());
    CodeInjectionForTesting.execute(APPEND_PLACEHOLDER,
        server.getId().toString(), null);
    raftLog.append(placeHolder);
    processor.start();
    senders.forEach(LogAppender::start);
    return placeHolder;
  }

  boolean isReady() {
    return server.getState().getLastAppliedIndex() >= placeHolderIndex;
  }

  void stop() {
    this.running = false;
    // do not interrupt event processor since it may be in the middle of logSync
    senders.forEach(LogAppender::stop);
    final NotLeaderException nle = server.generateNotLeaderException();
    final Collection<CommitInfoProto> commitInfos = server.getCommitInfos();
    try {
      final Collection<TransactionContext> transactions = pendingRequests.sendNotLeaderResponses(nle, commitInfos);
      server.getStateMachine().leaderEvent().notifyNotLeader(transactions);
      watchRequests.failWatches(nle);
    } catch (IOException e) {
      LOG.warn("{}: Caught exception in sendNotLeaderResponses", this, e);
    }
    messageStreamRequests.clear();
    server.getServerRpc().notifyNotLeader(server.getMemberId().getGroupId());
    logAppenderMetrics.unregister();
    raftServerMetrics.unregister();
    pendingRequests.close();
  }

  void notifySenders() {
    senders.forEach(LogAppender::notifyLogAppender);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  @Override
  public boolean onFollowerTerm(FollowerInfo follower, long followerTerm) {
    if (isAttendingVote(follower) && followerTerm > getCurrentTerm()) {
      submitStepDownEvent(followerTerm, StepDownReason.HIGHER_TERM);
      return true;
    }
    return false;
  }

  /**
   * Start bootstrapping new peers
   */
  PendingRequest startSetConfiguration(SetConfigurationRequest request) {
    LOG.info("{}: startSetConfiguration {}", this, request);
    Preconditions.assertTrue(running && !inStagingState());

    final List<RaftPeer> peersInNewConf = request.getPeersInNewConf();
    final Collection<RaftPeer> peersToBootStrap = server.getRaftConf().filterNotContainedInConf(peersInNewConf);

    // add the request to the pending queue
    final PendingRequest pending = pendingRequests.addConfRequest(request);

    ConfigurationStagingState configurationStagingState = new ConfigurationStagingState(
        peersToBootStrap, new PeerConfiguration(peersInNewConf));
    Collection<RaftPeer> newPeers = configurationStagingState.getNewPeers();
    // set the staging state
    this.stagingState = configurationStagingState;

    if (newPeers.isEmpty()) {
      applyOldNewConf();
    } else {
      // update the LeaderState's sender list
      addAndStartSenders(newPeers);
    }
    return pending;
  }

  PendingRequests.Permit tryAcquirePendingRequest(Message message) {
    return pendingRequests.tryAcquire(message);
  }

  PendingRequest addPendingRequest(PendingRequests.Permit permit, RaftClientRequest request, TransactionContext entry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: addPendingRequest at {}, entry={}", this, request,
          LogProtoUtils.toLogEntryString(entry.getLogEntry()));
    }
    return pendingRequests.add(permit, request, entry);
  }

  CompletableFuture<RaftClientReply> streamAsync(RaftClientRequest request) {
    return messageStreamRequests.streamAsync(request)
        .thenApply(dummy -> server.newSuccessReply(request))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftClientRequest request) {
    return messageStreamRequests.streamEndOfRequestAsync(request)
        .thenApply(bytes -> RaftClientRequest.toWriteRequest(request, Message.valueOf(bytes)));
  }

  CompletableFuture<RaftClientReply> addWatchReqeust(RaftClientRequest request) {
    LOG.debug("{}: addWatchRequest {}", this, request);
    return watchRequests.add(request)
        .thenApply(v -> server.newSuccessReply(request))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  private RaftClientReply exception2RaftClientReply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof NotReplicatedException) {
      final NotReplicatedException nre = (NotReplicatedException)e;
      return server.newReplyBuilder(request)
          .setException(nre)
          .setLogIndex(nre.getLogIndex())
          .build();
    } else if (e instanceof NotLeaderException) {
      return server.newExceptionReply(request, (NotLeaderException)e);
    } else if (e instanceof LeaderNotReadyException) {
      return server.newExceptionReply(request, (LeaderNotReadyException)e);
    } else {
      throw new CompletionException(e);
    }
  }

  @Override
  public void onFollowerCommitIndex(FollowerInfo follower, long commitIndex) {
    if (follower.updateCommitIndex(commitIndex)) {
      commitIndexChanged();
    }
  }

  private void commitIndexChanged() {
    getMajorityMin(FollowerInfo::getCommitIndex, raftLog::getLastCommittedIndex).ifPresent(m -> {
      // Normally, leader commit index is always ahead of followers.
      // However, after a leader change, the new leader commit index may
      // be behind some followers in the beginning.
      watchRequests.update(ReplicationLevel.ALL_COMMITTED, m.min);
      watchRequests.update(ReplicationLevel.MAJORITY_COMMITTED, m.majority);
      watchRequests.update(ReplicationLevel.MAJORITY, m.max);
    });
    notifySenders();
  }

  private void applyOldNewConf() {
    final ServerState state = server.getState();
    final RaftConfigurationImpl current = state.getRaftConf();
    final RaftConfigurationImpl oldNewConf= stagingState.generateOldNewConf(current, state.getLog().getNextIndex());
    // apply the (old, new) configuration to log, and use it as the current conf
    long index = state.getLog().append(state.getCurrentTerm(), oldNewConf);
    updateConfiguration(index, oldNewConf);

    this.stagingState = null;
    notifySenders();
  }

  private void updateConfiguration(long logIndex, RaftConfigurationImpl newConf) {
    Preconditions.assertTrue(logIndex == newConf.getLogEntryIndex());
    voterLists = divideFollowers(newConf);
    server.getState().setRaftConf(newConf);
  }

  void updateFollowerCommitInfos(CommitInfoCache cache, List<CommitInfoProto> protos) {
    for (LogAppender sender : senders.getSenders()) {
      FollowerInfo info = sender.getFollower();
      protos.add(cache.update(info.getPeer(), info.getCommitIndex()));
    }
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId) {
    final boolean initializing = isAttendingVote(follower);
    final RaftPeerId targetId = follower.getPeer().getId();
    return ServerProtoUtils.toAppendEntriesRequestProto(server.getMemberId(), targetId, currentTerm, entries,
        ServerImplUtils.effectiveCommitIndex(raftLog.getLastCommittedIndex(), previous, entries.size()),
        initializing, previous, server.getCommitInfos(), callId);
  }

  /**
   * Update sender list for setConfiguration request
   */
  void addAndStartSenders(Collection<RaftPeer> newPeers) {
    addSenders(newPeers, RaftLog.LEAST_VALID_LOG_INDEX, false).forEach(LogAppender::start);
  }

  Collection<LogAppender> addSenders(Collection<RaftPeer> newPeers, long nextIndex, boolean attendVote) {
    final Timestamp t = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final List<LogAppender> newAppenders = newPeers.stream()
        .map(peer -> {
          final FollowerInfo f = new FollowerInfoImpl(server.getMemberId(), peer, t, nextIndex, attendVote);
          LogAppender logAppender = server.newLogAppender(this, f);
          peerIdFollowerInfoMap.put(peer.getId(), f);
          raftServerMetrics.addFollower(peer.getId());
          logAppenderMetrics.addFollowerGauges(peer.getId(), f::getNextIndex, f::getMatchIndex, f::getLastRpcTime);
          return logAppender;
        }).collect(Collectors.toList());
    senders.addAll(newAppenders);
    return newAppenders;
  }

  void stopAndRemoveSenders(Predicate<LogAppender> predicate) {
    final List<LogAppender> toStop = senders.stream().filter(predicate).collect(Collectors.toList());
    toStop.forEach(LogAppender::stop);
    senders.removeAll(toStop);
  }

  @Override
  public void restart(LogAppender sender) {
    final FollowerInfo follower = sender.getFollower();
    LOG.info("{}: Restarting {} for {}", this, JavaUtils.getClassSimpleName(sender.getClass()), follower.getName());
    sender.stop();
    senders.removeAll(Collections.singleton(sender));
    addAndStartSenders(Collections.singleton(follower.getPeer()));
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders(RaftConfigurationImpl conf) {
    Preconditions.assertTrue(conf.isStable() && !inStagingState());
    stopAndRemoveSenders(s -> !conf.containsInConf(s.getFollowerId()));
  }

  void submitStepDownEvent(StepDownReason reason) {
    submitStepDownEvent(getCurrentTerm(), reason);
  }

  void submitStepDownEvent(long term, StepDownReason reason) {
    eventQueue.submit(new StateUpdateEvent(StateUpdateEvent.Type.STEP_DOWN, term, () -> stepDown(term, reason)));
  }

  private void stepDown(long term, StepDownReason reason) {
    try {
      server.changeToFollowerAndPersistMetadata(term, reason);
    } catch(IOException e) {
      final String s = this + ": Failed to persist metadata for term " + term;
      LOG.warn(s, e);
      // the failure should happen while changing the state to follower
      // thus the in-memory state should have been updated
      if (running) {
        throw new IllegalStateException(s + " and running == true", e);
      }
    }
  }

  private synchronized void sendStartLeaderElectionToHigherPriorityPeer(RaftPeerId follower, TermIndex lastEntry) {
    ServerState state = server.getState();
    TermIndex currLastEntry = state.getLastEntry();
    if (ServerState.compareLog(currLastEntry, lastEntry) != 0) {
      LOG.warn("{} can not send StartLeaderElectionRequest to follower:{} because currLastEntry:{} " +
              "did not match lastEntry:{}", this, follower, currLastEntry, lastEntry);
      return;
    }

    // leader must step down to support leader lease.
    stepDown(currentTerm, StepDownReason.HIGHER_PRIORITY);

    final StartLeaderElectionRequestProto r = ServerProtoUtils.toStartLeaderElectionRequestProto(
        server.getMemberId(), follower, lastEntry);
    CompletableFuture.supplyAsync(() -> {
      try {
        StartLeaderElectionReplyProto replyProto = server.getServerRpc().startLeaderElection(r);
        LOG.info("{} received {} reply of StartLeaderElectionRequest from follower:{}",
            this, replyProto.getServerReply().getSuccess() ? "success" : "fail", follower);
      } catch (IOException e) {
        LOG.warn("{} send StartLeaderElectionRequest throw exception", this, e);
      }
      return null;
    });
  }

  private void prepare() {
    synchronized (server) {
      if (running) {
        final ServerState state = server.getState();
        if (state.getRaftConf().isTransitional() && state.isConfCommitted()) {
          // the configuration is in transitional state, and has been committed
          // so it is time to generate and replicate (new) conf.
          replicateNewConf();
        }
      }
    }
  }

  /**
   * The processor thread takes the responsibility to update the raft server's
   * state, such as changing to follower, or updating the committed index.
   */
  private class EventProcessor extends Daemon {
    public EventProcessor(String name) {
      setName(name);
    }
    @Override
    public void run() {
      // apply an empty message; check if necessary to replicate (new) conf
      prepare();

      while (running) {
        final StateUpdateEvent event = eventQueue.poll();
        synchronized(server) {
          if (running) {
            if (event != null) {
              event.execute();
            } else if (inStagingState()) {
              checkStaging();
            } else {
              yieldLeaderToHigherPriorityPeer();
              checkLeadership();
            }
          }
        }
      }
    }
  }

  /**
   * So far we use a simple implementation for catchup checking:
   * 1. If the latest rpc time of the remote peer is before 3 * max_timeout,
   *    the peer made no progress for that long. We should fail the whole
   *    setConfiguration request.
   * 2. If the peer's matching index is just behind for a small gap, and the
   *    peer was updated recently (within max_timeout), declare the peer as
   *    caught-up.
   * 3. Otherwise the peer is making progressing. Keep waiting.
   */
  private BootStrapProgress checkProgress(FollowerInfo follower, long committed) {
    Preconditions.assertTrue(!isAttendingVote(follower));
    final Timestamp progressTime = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final Timestamp timeoutTime = Timestamp.currentTime().addTimeMs(-3L * server.getMaxTimeoutMs());
    if (follower.getLastRpcResponseTime().compareTo(timeoutTime) < 0) {
      LOG.debug("{} detects a follower {} timeout ({}) for bootstrapping", this, follower, timeoutTime);
      return BootStrapProgress.NOPROGRESS;
    } else if (follower.getMatchIndex() + stagingCatchupGap > committed
        && follower.getLastRpcResponseTime().compareTo(progressTime) > 0) {
      return BootStrapProgress.CAUGHTUP;
    } else {
      return BootStrapProgress.PROGRESSING;
    }
  }

  private Collection<BootStrapProgress> checkAllProgress(long committed) {
    Preconditions.assertTrue(inStagingState());
    return senders.stream()
        .filter(sender -> !isAttendingVote(sender.getFollower()))
        .map(sender -> checkProgress(sender.getFollower(), committed))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  @Override
  public void onFollowerSuccessAppendEntries(FollowerInfo follower) {
    if (isAttendingVote(follower)) {
      submitUpdateCommitEvent();
    } else {
      eventQueue.submit(checkStagingEvent);
    }
  }

  private void checkStaging() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done. Then, fallback to UPDATE_COMMIT
      updateCommitEvent.execute();
    } else {
      final long committedIndex = server.getState().getLog()
          .getLastCommittedIndex();
      Collection<BootStrapProgress> reports = checkAllProgress(committedIndex);
      if (reports.contains(BootStrapProgress.NOPROGRESS)) {
        stagingState.fail(BootStrapProgress.NOPROGRESS);
      } else if (!reports.contains(BootStrapProgress.PROGRESSING)) {
        // all caught up!
        applyOldNewConf();
        senders.stream()
            .map(LogAppender::getFollower)
            .map(FollowerInfoImpl.class::cast)
            .forEach(FollowerInfoImpl::startAttendVote);
      }
    }
  }

  boolean isBootStrappingPeer(RaftPeerId peerId) {
    return Optional.ofNullable(stagingState).map(s -> s.contains(peerId)).orElse(false);
  }

  void submitUpdateCommitEvent() {
    eventQueue.submit(updateCommitEvent);
  }

  static class MinMajorityMax {
    private final long min;
    private final long majority;
    private final long max;

    MinMajorityMax(long min, long majority, long max) {
      this.min = min;
      this.majority = majority;
      this.max = max;
    }

    MinMajorityMax combine(MinMajorityMax that) {
      return new MinMajorityMax(
          Math.min(this.min, that.min),
          Math.min(this.majority, that.majority),
          Math.min(this.max, that.max));
    }

    static MinMajorityMax valueOf(long[] sorted) {
      return new MinMajorityMax(sorted[0], getMajority(sorted), getMax(sorted));
    }

    static long getMajority(long[] sorted) {
      return sorted[(sorted.length - 1) / 2];
    }

    static long getMax(long[] sorted) {
      return sorted[sorted.length - 1];
    }
  }

  private void updateCommit() {
    getMajorityMin(FollowerInfo::getMatchIndex, raftLog::getFlushIndex)
        .ifPresent(m -> updateCommit(m.majority, m.min));
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex, LongSupplier logIndex) {
    final RaftPeerId selfId = server.getId();
    final RaftConfigurationImpl conf = server.getRaftConf();

    final List<RaftPeerId> followers = voterLists.get(0);
    final boolean includeSelf = conf.containsInConf(selfId);
    if (followers.isEmpty() && !includeSelf) {
      return Optional.empty();
    }

    final long[] indicesInNewConf = getSorted(followers, includeSelf, followerIndex, logIndex);
    final MinMajorityMax newConf = MinMajorityMax.valueOf(indicesInNewConf);

    if (!conf.isTransitional()) {
      return Optional.of(newConf);
    } else { // configuration is in transitional state
      final List<RaftPeerId> oldFollowers = voterLists.get(1);
      final boolean includeSelfInOldConf = conf.containsInOldConf(selfId);
      if (oldFollowers.isEmpty() && !includeSelfInOldConf) {
        return Optional.empty();
      }

      final long[] indicesInOldConf = getSorted(oldFollowers, includeSelfInOldConf, followerIndex, logIndex);
      final MinMajorityMax oldConf = MinMajorityMax.valueOf(indicesInOldConf);
      return Optional.of(newConf.combine(oldConf));
    }
  }

  private void updateCommit(LogEntryHeader[] entriesToCommit) {
    final long newCommitIndex = raftLog.getLastCommittedIndex();
    logMetadata(newCommitIndex);
    commitIndexChanged();

    boolean hasConfiguration = false;
    for (LogEntryHeader entry : entriesToCommit) {
      if (entry.getIndex() > newCommitIndex) {
        break;
      }
      hasConfiguration |= entry.getLogEntryBodyCase() == LogEntryBodyCase.CONFIGURATIONENTRY;
      raftLog.getRaftLogMetrics().onLogEntryCommitted(entry);
    }
    if (hasConfiguration) {
      checkAndUpdateConfiguration();
    }
  }

  private void updateCommit(long majority, long min) {
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
    if (majority > oldLastCommitted) {
      // Get the headers before updating commit index since the log can be purged after a snapshot
      final LogEntryHeader[] entriesToCommit = raftLog.getEntries(oldLastCommitted + 1, majority + 1);

      if (server.getState().updateCommitIndex(majority, currentTerm, true)) {
        updateCommit(entriesToCommit);
      }
    }
    watchRequests.update(ReplicationLevel.ALL, min);
  }

  private void logMetadata(long commitIndex) {
    raftLog.appendMetadata(currentTerm, commitIndex);
    notifySenders();
  }

  private void checkAndUpdateConfiguration() {
    final RaftConfigurationImpl conf = server.getRaftConf();
    if (conf.isTransitional()) {
      replicateNewConf();
    } else { // the (new) log entry has been committed
      pendingRequests.replySetConfiguration(server::newSuccessReply);
      // if the leader is not included in the current configuration, step down
      if (!conf.containsInConf(server.getId())) {
        LOG.info("{} is not included in the new configuration {}. Will shutdown server...", this, conf);
        try {
          // leave some time for all RPC senders to send out new conf entry
          server.properties().minRpcTimeout().sleep();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        // the pending request handler will send NotLeaderException for
        // pending client requests when it stops
        server.close();
      }
    }
  }

  /**
   * when the (old, new) log entry has been committed, should replicate (new):
   * 1) append (new) to log
   * 2) update conf to (new)
   * 3) update RpcSenders list
   * 4) start replicating the log entry
   */
  private void replicateNewConf() {
    final RaftConfigurationImpl conf = server.getRaftConf();
    final RaftConfigurationImpl newConf = RaftConfigurationImpl.newBuilder()
        .setConf(conf)
        .setLogEntryIndex(raftLog.getNextIndex())
        .build();
    // stop the LogAppender if the corresponding follower is no longer in the conf
    updateSenders(newConf);
    long index = raftLog.append(server.getState().getCurrentTerm(), newConf);
    updateConfiguration(index, newConf);
    notifySenders();
  }

  private List<FollowerInfo> getFollowerInfos(List<RaftPeerId> followerIDs) {
    List<FollowerInfo> followerInfos = new ArrayList<>();
    for (int i = 0; i < followerIDs.size(); i++) {
      RaftPeerId id = followerIDs.get(i);
      if (!peerIdFollowerInfoMap.containsKey(id)) {
        throw new IllegalArgumentException("RaftPeerId:" + id +
                " not in peerIdFollowerInfoMap of leader:" + server.getMemberId());
      }

      followerInfos.add(peerIdFollowerInfoMap.get(id));
    }

    return followerInfos;
  }

  private long[] getSorted(List<RaftPeerId> followerIDs, boolean includeSelf,
      ToLongFunction<FollowerInfo> getFollowerIndex, LongSupplier getLogIndex) {
    final int length = includeSelf ? followerIDs.size() + 1 : followerIDs.size();
    if (length == 0) {
      throw new IllegalArgumentException("followers.size() == "
          + followerIDs.size() + " and includeSelf == " + includeSelf);
    }

    final long[] indices = new long[length];
    List<FollowerInfo> followerInfos = getFollowerInfos(followerIDs);
    for (int i = 0; i < followerInfos.size(); i++) {
      indices[i] = getFollowerIndex.applyAsLong(followerInfos.get(i));
    }

    if (includeSelf) {
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = getLogIndex.getAsLong();
    }

    Arrays.sort(indices);
    return indices;
  }

  private List<List<RaftPeerId>> divideFollowers(RaftConfigurationImpl conf) {
    List<List<RaftPeerId>> lists = new ArrayList<>(2);
    List<RaftPeerId> listForNew = senders.stream()
        .map(LogAppender::getFollowerId)
        .filter(conf::containsInConf)
        .collect(Collectors.toList());
    lists.add(listForNew);
    if (conf.isTransitional()) {
      List<RaftPeerId> listForOld = senders.stream()
          .map(LogAppender::getFollowerId)
          .filter(conf::containsInOldConf)
          .collect(Collectors.toList());
      lists.add(listForOld);
    }
    return lists;
  }

  private void yieldLeaderToHigherPriorityPeer() {
    if (!server.getInfo().isLeader()) {
      return;
    }

    final RaftConfigurationImpl conf = server.getRaftConf();
    int leaderPriority = conf.getPeer(server.getId()).getPriority();

    for (LogAppender logAppender : senders.getSenders()) {
      FollowerInfo followerInfo = logAppender.getFollower();
      RaftPeerId followerID = followerInfo.getPeer().getId();
      int followerPriority = conf.getPeer(followerID).getPriority();

      if (followerPriority <= leaderPriority) {
        continue;
      }

      final TermIndex leaderLastEntry = server.getState().getLastEntry();
      if (leaderLastEntry == null) {
        LOG.info("{} send StartLeaderElectionRequest to follower:{} on term:{} because follower's priority:{} " +
                "is higher than leader's:{} and leader's lastEntry is null",
            this, followerID, currentTerm, followerPriority, leaderPriority);

        sendStartLeaderElectionToHigherPriorityPeer(followerID, null);
        return;
      }

      if (followerInfo.getMatchIndex() >= leaderLastEntry.getIndex()) {
        LOG.info("{} send StartLeaderElectionRequest to follower:{} on term:{} because follower's priority:{} " +
                "is higher than leader's:{} and follower's lastEntry index:{} catch up with leader's:{}",
            this, followerID, currentTerm, followerPriority, leaderPriority, followerInfo.getMatchIndex(),
            leaderLastEntry.getIndex());

        sendStartLeaderElectionToHigherPriorityPeer(followerID, leaderLastEntry);
        return;
      }
    }
  }

  boolean isLeaderLeaseValid() {
    if (checkLeaderLease()) {
      return true;
    }

    updateLeaderLease();
    return checkLeaderLease();
  }

  private boolean checkLeaderLease() {
    return lastLeaderTimestamp.get().elapsedTimeMs() < server.properties().leaderLeaseTimeoutMs();
  }

  private void updateLeaderLease() {
    List<RaftPeerId> activePeers = new ArrayList<>();
    List<FollowerInfo> followerInfos = getFollowerInfos();
    List<Timestamp> lastRpcSendTimeWithResponseList = new ArrayList<>();
    for (final FollowerInfo info : followerInfos) {
      Timestamp lastRpcSendTimeWithResponse = info.getLastRpcSendTimeWithResponse();
      lastRpcSendTimeWithResponseList.add(lastRpcSendTimeWithResponse);
      if (lastRpcSendTimeWithResponse.elapsedTimeMs() <= server.properties().leaderLeaseTimeoutMs()) {
        activePeers.add(info.getPeer().getId());
      }
    }

    final RaftConfigurationImpl conf = server.getRaftConf();
    if (conf.hasMajority(activePeers, server.getId())) {
      // leader lease check passed
      if (lastRpcSendTimeWithResponseList.size() > 0) {
        Collections.sort(lastRpcSendTimeWithResponseList);
        Timestamp startLease = lastRpcSendTimeWithResponseList.get(lastRpcSendTimeWithResponseList.size() / 2);
        lastLeaderTimestamp.set(startLease);
      } else {
        lastLeaderTimestamp.set(Timestamp.currentTime());
      }
    } else {
      LOG.warn("{} Can not update leader lease on term: {} lease timeout: {}ms conf: {}",
          this, currentTerm, server.properties().leaderLeaseTimeoutMs(), conf);
    }
  }

  private AtomicReference<Timestamp> lastLeaderTimestamp = new AtomicReference<>(Timestamp.currentTime());

  /**
   * See the thesis section 6.2: A leader in Raft steps down
   * if an election timeout elapses without a successful
   * round of heartbeats to a majority of its cluster.
   */

  public boolean checkLeadership() {
    if (!server.getInfo().isLeader()) {
      return false;
    }
    // The initial value of lastRpcResponseTime in FollowerInfo is set by
    // LeaderState::addSenders(), which is fake and used to trigger an
    // immediate round of AppendEntries request. Since candidates collect
    // votes from majority before becoming leader, without seeing higher term,
    // ideally, A leader is legal for election timeout if become leader soon.
    if (server.getRole().getRoleElapsedTimeMs() < server.getMaxTimeoutMs()) {
      return true;
    }

    final List<RaftPeerId> activePeers = senders.stream()
        .filter(sender -> sender.getFollower()
                                .getLastRpcResponseTime()
                                .elapsedTimeMs() <= server.getMaxTimeoutMs())
        .map(LogAppender::getFollowerId)
        .collect(Collectors.toList());

    final RaftConfigurationImpl conf = server.getRaftConf();

    if (conf.hasMajority(activePeers, server.getId())) {
      // leadership check passed
      return true;
    }

    LOG.warn("{} Lost leadership on term: {} Election timeout: {}ms. In charge for: {}ms. Conf: {}",
        this, currentTerm, server.getMaxTimeoutMs(), server.getRole().getRoleElapsedTimeMs(), conf);
    senders.stream().map(LogAppender::getFollower).forEach(f -> LOG.warn("Follower {}", f));

    // step down as follower
    stepDown(currentTerm, StepDownReason.LOST_MAJORITY_HEARTBEATS);
    return false;
  }

  void replyPendingRequest(long logIndex, RaftClientReply reply) {
    pendingRequests.replyPendingRequest(logIndex, reply);
  }

  TransactionContext getTransactionContext(long index) {
    return pendingRequests.getTransactionContext(index);
  }

  long[] getFollowerNextIndices() {
    return senders.stream().mapToLong(s -> s.getFollower().getNextIndex()).toArray();
  }

  private class ConfigurationStagingState {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final Map<RaftPeerId, RaftPeer> newPeers;
    private final PeerConfiguration newConf;

    ConfigurationStagingState(Collection<RaftPeer> newPeers,
        PeerConfiguration newConf) {
      Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      for (RaftPeer peer : newPeers) {
        map.put(peer.getId(), peer);
      }
      this.newPeers = Collections.unmodifiableMap(map);
      this.newConf = newConf;
    }

    RaftConfigurationImpl generateOldNewConf(RaftConfigurationImpl current, long logIndex) {
      return RaftConfigurationImpl.newBuilder()
          .setConf(newConf)
          .setOldConf(current)
          .setLogEntryIndex(logIndex)
          .build();
    }

    Collection<RaftPeer> getNewPeers() {
      return newPeers.values();
    }

    boolean contains(RaftPeerId peerId) {
      return newPeers.containsKey(peerId);
    }

    void fail(BootStrapProgress progress) {
      final String message = this + ": Fail to set configuration " + newConf + " due to " + progress;
      LOG.debug(message);
      stopAndRemoveSenders(s -> !isAttendingVote(s.getFollower()));

      stagingState = null;
      // send back failure response to client's request
      pendingRequests.failSetConfiguration(new ReconfigurationTimeoutException(message));
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * @return the RaftPeer (address and id) information of the followers.
   */
  List<RaftPeer> getFollowers() {
    return Collections.unmodifiableList(senders.stream()
        .map(sender -> sender.getFollower().getPeer())
        .collect(Collectors.toList()));
  }

  List<FollowerInfo> getFollowerInfos() {
    return Collections.unmodifiableList(senders.stream()
        .map(sender -> sender.getFollower())
        .collect(Collectors.toList()));
  }

  Stream<LogAppender> getLogAppenders() {
    return senders.stream();
  }

  private static boolean isAttendingVote(FollowerInfo follower) {
    return ((FollowerInfoImpl)follower).isAttendingVote();
  }

  @Override
  public void checkHealth(FollowerInfo follower) {
    final TimeDuration elapsedTime = follower.getLastRpcResponseTime().elapsedTime();
    if (elapsedTime.compareTo(server.properties().rpcSlownessTimeout()) > 0) {
      server.getStateMachine().leaderEvent().notifyFollowerSlowness(server.getInfo().getRoleInfoProto());
    }
    final RaftPeerId followerId = follower.getPeer().getId();
    raftServerMetrics.recordFollowerHeartbeatElapsedTime(followerId, elapsedTime.toLong(TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    return name;
  }
}
