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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import com.codahale.metrics.Timer;

/**
 * This class tracks the log entries that have been committed in a quorum and
 * applies them to the state machine. We let a separate thread do this work
 * asynchronously so that this will not block normal raft protocol.
 *
 * If the auto log compaction is enabled, the state machine updater thread will
 * trigger a snapshot of the state machine by calling
 * {@link StateMachine#takeSnapshot} when the log size exceeds a limit.
 */
class StateMachineUpdater implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(StateMachineUpdater.class);

  enum State {
    RUNNING, STOP, RELOAD, EXCEPTION
  }

  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;
  private final String name;

  private final StateMachine stateMachine;
  private final RaftServerImpl server;
  private final RaftLog raftLog;

  private final Long autoSnapshotThreshold;
  private final boolean purgeUptoSnapshotIndex;

  private final Thread updater;
  private final RaftLogIndex appliedIndex;
  private final RaftLogIndex snapshotIndex;
  private final AtomicReference<Long> stopIndex = new AtomicReference<>();
  private volatile State state = State.RUNNING;
  private SnapshotRetentionPolicy snapshotRetentionPolicy;
  private StateMachineMetrics stateMachineMetrics = null;

  StateMachineUpdater(StateMachine stateMachine, RaftServerImpl server,
      ServerState serverState, long lastAppliedIndex, RaftProperties properties) {
    this.name = serverState.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.stateMachine = stateMachine;
    this.server = server;
    this.raftLog = serverState.getLog();

    this.appliedIndex = new RaftLogIndex("appliedIndex", lastAppliedIndex);
    this.snapshotIndex = new RaftLogIndex("snapshotIndex", lastAppliedIndex);

    final boolean autoSnapshot = RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties);
    this.autoSnapshotThreshold = autoSnapshot? RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties): null;
    final int numSnapshotFilesRetained = RaftServerConfigKeys.Snapshot.retentionFileNum(properties);
    this.snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return numSnapshotFilesRetained;
      }
    };
    this.purgeUptoSnapshotIndex = RaftServerConfigKeys.Log.purgeUptoSnapshotIndex(properties);

    updater = new Daemon(this);
  }

  void start() {
    //wait for RaftServerImpl and ServerState constructors to complete
    initializeMetrics();
    updater.start();
  }

  private void initializeMetrics() {
    if (stateMachineMetrics == null) {
      stateMachineMetrics =
          StateMachineMetrics.getStateMachineMetrics(
              server, appliedIndex, stateMachine);
    }
  }

  private void stop() {
    state = State.STOP;
    try {
      stateMachine.close();
      stateMachineMetrics.unregister();
    } catch(Throwable t) {
      LOG.warn(name + ": Failed to close " + JavaUtils.getClassSimpleName(stateMachine.getClass())
          + " " + stateMachine, t);
    }
  }

  /**
   * Stop the updater thread after all the committed transactions
   * have been applied to the state machine.
   */
  void stopAndJoin() throws InterruptedException {
    if (state == State.EXCEPTION) {
      stop();
      return;
    }
    if (stopIndex.compareAndSet(null, raftLog.getLastCommittedIndex())) {
      notifyUpdater();
      LOG.info("{}: set stopIndex = {}", this, stopIndex);
    }
    updater.join();
  }

  void reloadStateMachine() {
    state = State.RELOAD;
    notifyUpdater();
  }

  @SuppressFBWarnings("NN_NAKED_NOTIFY")
  synchronized void notifyUpdater() {
    notifyAll();
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void run() {
    for(; state != State.STOP; ) {
      try {
        waitForCommit();

        if (state == State.RELOAD) {
          reload();
        }

        final MemoizedSupplier<List<CompletableFuture<Message>>> futures = applyLog();
        checkAndTakeSnapshot(futures);

        if (shouldStop()) {
          checkAndTakeSnapshot(futures);
          stop();
        }
      } catch (Throwable t) {
        if (t instanceof InterruptedException && state == State.STOP) {
          LOG.info("{} was interrupted.  Exiting ...", this);
        } else {
          state = State.EXCEPTION;
          LOG.error(this + " caught a Throwable.", t);
          server.close();
        }
      }
    }
  }

  private synchronized void waitForCommit() throws InterruptedException {
    // When a peer starts, the committed is initialized to 0.
    // It will be updated only after the leader contacts other peers.
    // Thus it is possible to have applied > committed initially.
    final long applied = getLastAppliedIndex();
    for(; applied >= raftLog.getLastCommittedIndex() && state == State.RUNNING && !shouldStop(); ) {
      wait();
    }
  }

  private void reload() throws IOException {
    LifeCycle.State current = stateMachine.getLifeCycleState();

    Preconditions.assertTrue(current == LifeCycle.State.NEW || current == LifeCycle.State.PAUSED);

    stateMachine.reinitialize();

    final SnapshotInfo snapshot = stateMachine.getLatestSnapshot();
    Objects.requireNonNull(snapshot, "snapshot == null");
    final long i = snapshot.getIndex();
    snapshotIndex.setUnconditionally(i, infoIndexChange);
    appliedIndex.setUnconditionally(i, infoIndexChange);
    state = State.RUNNING;
  }

  private MemoizedSupplier<List<CompletableFuture<Message>>> applyLog() throws RaftLogIOException {
    final MemoizedSupplier<List<CompletableFuture<Message>>> futures = MemoizedSupplier.valueOf(ArrayList::new);
    final long committed = raftLog.getLastCommittedIndex();
    for(long applied; (applied = getLastAppliedIndex()) < committed && state == State.RUNNING && !shouldStop(); ) {
      final long nextIndex = applied + 1;
      final LogEntryProto next = raftLog.get(nextIndex);
      if (next != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: applying nextIndex={}, nextLog={}", this, nextIndex, LogProtoUtils.toLogEntryString(next));
        } else {
          LOG.debug("{}: applying nextIndex={}", this, nextIndex);
        }

        final CompletableFuture<Message> f = server.applyLogToStateMachine(next);
        if (f != null) {
          futures.get().add(f);
        }
        final long incremented = appliedIndex.incrementAndGet(debugIndexChange);
        Preconditions.assertTrue(incremented == nextIndex);
      } else {
        LOG.debug("{}: logEntry {} is null. There may be snapshot to load. state:{}",
            this, nextIndex, state);
        break;
      }
    }
    return futures;
  }

  private void checkAndTakeSnapshot(MemoizedSupplier<List<CompletableFuture<Message>>> futures)
      throws ExecutionException, InterruptedException {
    // check if need to trigger a snapshot
    if (shouldTakeSnapshot()) {
      if (futures.isInitialized()) {
        JavaUtils.allOf(futures.get()).get();
      }

      takeSnapshot();
    }
  }

  private void takeSnapshot() {
    final long i;
    try {
      Timer.Context takeSnapshotTimerContext = stateMachineMetrics.getTakeSnapshotTimer().time();
      i = stateMachine.takeSnapshot();
      takeSnapshotTimerContext.stop();

      final long lastAppliedIndex = getLastAppliedIndex();
      if (i > lastAppliedIndex) {
        throw new StateMachineException(
            "Bug in StateMachine: snapshot index = " + i + " > appliedIndex = " + lastAppliedIndex
            + "; StateMachine class=" +  stateMachine.getClass().getName() + ", stateMachine=" + stateMachine);
      }
      stateMachine.getStateMachineStorage().cleanupOldSnapshots(snapshotRetentionPolicy);
    } catch (IOException e) {
      LOG.error(name + ": Failed to take snapshot", e);
      return;
    }

    if (i >= 0) {
      LOG.info("{}: Took a snapshot at index {}", name, i);
      snapshotIndex.updateIncreasingly(i, infoIndexChange);

      final long purgeIndex;
      if (purgeUptoSnapshotIndex) {
        // We can purge up to snapshot index even if all the peers do not have
        // commitIndex up to this snapshot index.
        purgeIndex = i;
      } else {
        final LongStream commitIndexStream = server.getCommitInfos().stream().mapToLong(
            CommitInfoProto::getCommitIndex);
        purgeIndex = LongStream.concat(LongStream.of(i), commitIndexStream).min().orElse(i);
      }
      raftLog.purge(purgeIndex);
    }
  }

  private boolean shouldStop() {
    return Optional.ofNullable(stopIndex.get()).filter(i -> i <= getLastAppliedIndex()).isPresent();
  }

  private boolean shouldTakeSnapshot() {
    if (autoSnapshotThreshold == null) {
      return false;
    } else if (shouldStop()) {
      return getLastAppliedIndex() - snapshotIndex.get() > 0;
    }
    return state == State.RUNNING && getLastAppliedIndex() - snapshotIndex.get() >= autoSnapshotThreshold;
  }

  private long getLastAppliedIndex() {
    return appliedIndex.get();
  }

  long getStateMachineLastAppliedIndex() {
    return stateMachine.getLastAppliedTermIndex().getIndex();
  }
}
