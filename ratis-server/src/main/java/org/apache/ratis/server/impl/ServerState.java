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
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.memory.MemoryRaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.SnapshotManager;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.ratis.server.impl.RaftServerImpl.LOG;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState implements Closeable {
  private final RaftGroupMemberId memberId;
  private final RaftServerImpl server;
  /** Raft log */
  private final RaftLog log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;
  /** The thread that applies committed log entries to the state machine */
  private final StateMachineUpdater stateMachineUpdater;
  /** local storage for log and snapshot */
  private final RaftStorage storage;
  private final SnapshotManager snapshotManager;
  private volatile Timestamp lastNoLeaderTime;
  private final TimeDuration noLeaderTimeout;

  /**
   * Latest term server has seen.
   * Initialized to 0 on first boot, increases monotonically.
   *
   * @see TermIndex#isValidTerm(int)
   */
  private final AtomicLong currentTerm = new AtomicLong();
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private volatile RaftPeerId leaderId;
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private volatile RaftPeerId votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private final AtomicReference<TermIndex> latestInstalledSnapshot = new AtomicReference<>();

  ServerState(RaftPeerId id, RaftGroup group, RaftProperties prop,
              RaftServerImpl server, StateMachine stateMachine)
      throws IOException {
    this.memberId = RaftGroupMemberId.valueOf(id, group.getGroupId());
    this.server = server;
    RaftConfiguration initialConf = RaftConfiguration.newBuilder()
        .setConf(group.getPeers()).build();
    configurationManager = new ConfigurationManager(initialConf);
    LOG.info("{}: {}", getMemberId(), configurationManager);

    // use full uuid string to create a subdirectory
    final File dir = chooseStorageDir(RaftServerConfigKeys.storageDir(prop),
        group.getGroupId().getUuid().toString());
    storage = new RaftStorage(dir, RaftServerConstants.StartupOption.REGULAR,
        RaftServerConfigKeys.Log.corruptionPolicy(prop));
    snapshotManager = new SnapshotManager(storage, id);

    long lastApplied = initStatemachine(stateMachine, group.getGroupId());

    // On start the leader is null, start the clock now
    leaderId = null;
    this.lastNoLeaderTime = Timestamp.currentTime();
    this.noLeaderTimeout = RaftServerConfigKeys.Notification.noLeaderTimeout(prop);

    // we cannot apply log entries to the state machine in this step, since we
    // do not know whether the local log entries have been committed.
    this.log = initRaftLog(getMemberId(), server, storage, this::setRaftConf, lastApplied, prop);

    RaftLog.Metadata metadata = log.loadMetadata();
    currentTerm.set(metadata.getTerm());
    votedFor = metadata.getVotedFor();

    stateMachineUpdater = new StateMachineUpdater(stateMachine, server, this, lastApplied, prop);
  }

  RaftGroupMemberId getMemberId() {
    return memberId;
  }

  static File chooseStorageDir(List<File> volumes, String targetSubDir) throws IOException {
    final Map<File, Integer> numberOfStorageDirPerVolume = new HashMap<>();
    final File[] empty = {};
    final List<File> resultList = new ArrayList<>();
    volumes.stream().flatMap(volume -> {
      final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(empty);
      numberOfStorageDirPerVolume.put(volume, dirs.length);
      return Arrays.stream(dirs);
    }).filter(dir -> targetSubDir.equals(dir.getName()))
        .forEach(resultList::add);

    if (resultList.size() > 1) {
      throw new IOException("More than one directories found for " + targetSubDir + ": " + resultList);
    }
    if (resultList.size() == 1) {
      return resultList.get(0);
    }
    return numberOfStorageDirPerVolume.entrySet().stream()
        .min(Comparator.comparing(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .map(v -> new File(v, targetSubDir))
        .orElseThrow(() -> new IOException("No storage directory found."));
  }

  private long initStatemachine(StateMachine sm, RaftGroupId gid)
      throws IOException {
    sm.initialize(server.getProxy(), gid, storage);
    SnapshotInfo snapshot = sm.getLatestSnapshot();

    if (snapshot == null || snapshot.getTermIndex().getIndex() < 0) {
      return RaftLog.INVALID_LOG_INDEX;
    }

    // get the raft configuration from raft metafile
    RaftConfiguration raftConf = storage.readRaftConfiguration();
    if (raftConf != null) {
      setRaftConf(raftConf.getLogEntryIndex(), raftConf);
    }
    return snapshot.getIndex();
  }

  void writeRaftConfiguration(LogEntryProto conf) {
    storage.writeRaftConfiguration(conf);
  }

  void start() {
    stateMachineUpdater.start();
  }

  private static RaftLog initRaftLog(RaftGroupMemberId memberId, RaftServerImpl server, RaftStorage storage,
      Consumer<LogEntryProto> logConsumer, long lastIndexInSnapshot, RaftProperties prop) throws IOException {
    final RaftLog log;
    if (RaftServerConfigKeys.Log.useMemory(prop)) {
      log = new MemoryRaftLog(memberId, lastIndexInSnapshot, prop);
    } else {
      log = new SegmentedRaftLog(memberId, server, storage, lastIndexInSnapshot, prop);
    }
    log.open(lastIndexInSnapshot, logConsumer);
    return log;
  }

  RaftConfiguration getRaftConf() {
    return configurationManager.getCurrent();
  }

  public long getCurrentTerm() {
    return currentTerm.get();
  }

  boolean updateCurrentTerm(long newTerm) {
    final long current = currentTerm.getAndUpdate(curTerm -> Math.max(curTerm, newTerm));
    if (newTerm > current) {
      votedFor = null;
      setLeader(null, "updateCurrentTerm");
      return true;
    }
    return false;
  }

  RaftPeerId getLeaderId() {
    return leaderId;
  }

  boolean hasLeader() {
    return leaderId != null;
  }

  /**
   * Become a candidate and start leader election
   */
  long initElection() {
    votedFor = getMemberId().getPeerId();
    setLeader(null, "initElection");
    return currentTerm.incrementAndGet();
  }

  void persistMetadata() throws IOException {
    this.log.writeMetadata(currentTerm.get(), votedFor);
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(RaftPeerId candidateId) {
    votedFor = candidateId;
    setLeader(null, "grantVote");
  }

  void setLeader(RaftPeerId newLeaderId, String op) {
    if (!Objects.equals(leaderId, newLeaderId)) {
      String suffix;
      if (newLeaderId == null) {
        // reset the time stamp when a null leader is assigned
        lastNoLeaderTime = Timestamp.currentTime();
        suffix = "";
      } else {
        Timestamp previous = lastNoLeaderTime;
        lastNoLeaderTime = null;
        suffix = ", leader elected after " + previous.elapsedTimeMs() + "ms";
        server.getStateMachine().notifyLeaderChanged(getMemberId(), newLeaderId);
      }
      LOG.info("{}: change Leader from {} to {} at term {} for {}{}",
          getMemberId(), leaderId, newLeaderId, getCurrentTerm(), op, suffix);
      leaderId = newLeaderId;
    }
  }

  boolean shouldNotifyExtendedNoLeader() {
    return Optional.ofNullable(lastNoLeaderTime)
        .map(Timestamp::elapsedTime)
        .filter(t -> t.compareTo(noLeaderTimeout) > 0)
        .isPresent();
  }

  public long getLastLeaderElapsedTimeMs() {
    final Timestamp t = lastNoLeaderTime;
    return t == null ? 0 : t.elapsedTimeMs();
  }

  void becomeLeader() {
    setLeader(getMemberId().getPeerId(), "becomeLeader");
  }

  public RaftLog getLog() {
    return log;
  }

  public TermIndex getLastEntry() {
    TermIndex lastEntry = getLog().getLastEntryTermIndex();
    if (lastEntry == null) {
      // lastEntry may need to be derived from snapshot
      SnapshotInfo snapshot = getLatestSnapshot();
      if (snapshot != null) {
        lastEntry = snapshot.getTermIndex();
      }
    }

    return lastEntry;
  }

  void appendLog(TransactionContext operation) throws StateMachineException {
    log.append(currentTerm.get(), operation);
    Objects.requireNonNull(operation.getLogEntry());
  }

  /**
   * Check if accept the leader selfId and term from the incoming AppendEntries rpc.
   * If accept, update the current state.
   * @return true if the check passes
   */
  boolean recognizeLeader(RaftPeerId peerLeaderId, long leaderTerm) {
    final long current = currentTerm.get();
    if (leaderTerm < current) {
      return false;
    } else if (leaderTerm > current || this.leaderId == null) {
      // If the request indicates a term that is greater than the current term
      // or no leader has been set for the current term, make sure to update
      // leader and term later
      return true;
    }
    return this.leaderId.equals(peerLeaderId);
  }

  /**
   * Check if the candidate's term is acceptable
   */
  boolean recognizeCandidate(RaftPeerId candidateId, long candidateTerm) {
    if (!getRaftConf().containsInConf(candidateId)) {
      return false;
    }
    final long current = currentTerm.get();
    if (candidateTerm > current) {
      return true;
    } else if (candidateTerm == current) {
      // has not voted yet or this is a retry
      return votedFor == null || votedFor.equals(candidateId);
    }
    return false;
  }

  static int compareLog(TermIndex lastEntry, TermIndex candidateLastEntry) {
    if (lastEntry == null) {
      // If the lastEntry of candidate is null, the proto will transfer an empty TermIndexProto,
      // then term and index of candidateLastEntry will both be 0.
      // Besides, candidateLastEntry comes from proto now, it never be null.
      // But we still check candidateLastEntry == null here,
      // to avoid candidateLastEntry did not come from proto in future.
      if (candidateLastEntry == null ||
          (candidateLastEntry.getTerm() == 0 && candidateLastEntry.getIndex() == 0)) {
        return 0;
      }
      return -1;
    } else if (candidateLastEntry == null) {
      return 1;
    }

    return lastEntry.compareTo(candidateLastEntry);
  }

  @Override
  public String toString() {
    return getMemberId() + ":t" + currentTerm + ", leader=" + leaderId
        + ", voted=" + votedFor + ", raftlog=" + log + ", conf=" + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommittedIndex() >=
        getRaftConf().getLogEntryIndex();
  }

  void setRaftConf(LogEntryProto entry) {
    if (entry.hasConfigurationEntry()) {
      setRaftConf(entry.getIndex(), ServerProtoUtils.toRaftConfiguration(entry));
    }
  }

  void setRaftConf(long logIndex, RaftConfiguration conf) {
    configurationManager.addConfiguration(logIndex, conf);
    server.getServerRpc().addPeers(conf.getPeers());
    LOG.info("{}: set configuration {} at {}", getMemberId(), conf, logIndex);
    LOG.trace("{}: {}", getMemberId(), configurationManager);
  }

  void updateConfiguration(LogEntryProto[] entries) {
    if (entries != null && entries.length > 0) {
      configurationManager.removeConfigurations(entries[0].getIndex());
      Arrays.stream(entries).forEach(this::setRaftConf);
    }
  }

  boolean updateStatemachine(long majorityIndex, long curTerm) {
    if (log.updateLastCommitted(majorityIndex, curTerm)) {
      stateMachineUpdater.notifyUpdater();
      return true;
    }
    return false;
  }

  void reloadStateMachine(long lastIndexInSnapshot) {
    log.updateSnapshotIndex(lastIndexInSnapshot);
    stateMachineUpdater.reloadStateMachine();
  }

  @Override
  public void close() throws IOException {
    try {
      stateMachineUpdater.stopAndJoin();
    } catch (InterruptedException e) {
      LOG.warn("{}: Interrupted when joining stateMachineUpdater", getMemberId(), e);
    }
    LOG.info("{}: closes. applyIndex: {}", getMemberId(), getLastAppliedIndex());

    log.close();
    storage.close();
  }

  public RaftStorage getStorage() {
    return storage;
  }

  void installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    // TODO: verify that we need to install the snapshot
    StateMachine sm = server.getStateMachine();
    sm.pause(); // pause the SM to prepare for install snapshot
    snapshotManager.installSnapshot(sm, request);
    updateInstalledSnapshotIndex(ServerProtoUtils.toTermIndex(request.getSnapshotChunk().getTermIndex()));
  }

  void updateInstalledSnapshotIndex(TermIndex lastTermIndexInSnapshot) {
    log.syncWithSnapshot(lastTermIndexInSnapshot.getIndex());
    latestInstalledSnapshot.set(lastTermIndexInSnapshot);
  }

  SnapshotInfo getLatestSnapshot() {
    return server.getStateMachine().getLatestSnapshot();
  }

  public long getLatestInstalledSnapshotIndex() {
    final TermIndex ti = latestInstalledSnapshot.get();
    return ti != null? ti.getIndex(): 0L;
  }

  /**
   * The last index included in either the latestSnapshot or the latestInstalledSnapshot
   * @return the last snapshot index
   */
  long getSnapshotIndex() {
    final SnapshotInfo s = getLatestSnapshot();
    final long latestSnapshotIndex = s != null ? s.getIndex() : 0;
    return Math.max(latestSnapshotIndex, getLatestInstalledSnapshotIndex());
  }

  public long getNextIndex() {
    final long logNextIndex = log.getNextIndex();
    final long snapshotNextIndex = getSnapshotIndex() + 1;
    return Math.max(logNextIndex, snapshotNextIndex);
  }

  public long getLastAppliedIndex() {
    return stateMachineUpdater.getStateMachineLastAppliedIndex();
  }

  boolean containsTermIndex(TermIndex ti) {
    Objects.requireNonNull(ti, "ti == null");

    if (Optional.ofNullable(latestInstalledSnapshot.get()).filter(ti::equals).isPresent()) {
      return true;
    }
    if (Optional.ofNullable(getLatestSnapshot()).map(SnapshotInfo::getTermIndex).filter(ti::equals).isPresent()) {
      return true;
    }
    return log.contains(ti);
  }
}
