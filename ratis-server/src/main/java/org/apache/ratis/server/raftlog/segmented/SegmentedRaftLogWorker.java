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
package org.apache.ratis.server.raftlog.segmented;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.metrics.RaftLogMetrics;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.SegmentFileInfo;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncationSegments;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog.Task;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.DataBlockingQueue;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This class takes the responsibility of all the raft log related I/O ops for a
 * raft peer.
 */
class SegmentedRaftLogWorker implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogWorker.class);

  static final TimeDuration ONE_SECOND = TimeDuration.valueOf(1, TimeUnit.SECONDS);

  static class StateMachineDataPolicy {
    private final boolean sync;
    private final TimeDuration syncTimeout;
    private final int syncTimeoutRetry;

    StateMachineDataPolicy(RaftProperties properties) {
      this.sync = RaftServerConfigKeys.Log.StateMachineData.sync(properties);
      this.syncTimeout = RaftServerConfigKeys.Log.StateMachineData.syncTimeout(properties);
      this.syncTimeoutRetry = RaftServerConfigKeys.Log.StateMachineData.syncTimeoutRetry(properties);
      Preconditions.assertTrue(syncTimeoutRetry >= -1);
    }

    boolean isSync() {
      return sync;
    }

    void getFromFuture(CompletableFuture<?> future, Supplier<Object> getName) throws IOException {
      Preconditions.assertTrue(isSync());
      TimeoutIOException lastException = null;
      for(int retry = 0; syncTimeoutRetry == -1 || retry <= syncTimeoutRetry; retry++) {
        try {
          IOUtils.getFromFuture(future, getName, syncTimeout);
          return;
        } catch(TimeoutIOException e) {
          LOG.warn("Timeout " + retry + (syncTimeoutRetry == -1? "/~": "/" + syncTimeoutRetry), e);
          lastException = e;
        }
      }
      Objects.requireNonNull(lastException, "lastException == null");
      throw lastException;
    }
  }

  static class WriteLogTasks {
    private final Queue<WriteLog> q = new LinkedList<>();
    private volatile long index;

    void offerOrCompleteFuture(WriteLog writeLog) {
      if (writeLog.getEndIndex() <= index || !offer(writeLog)) {
        writeLog.completeFuture();
      }
    }

    private synchronized boolean offer(WriteLog writeLog) {
      if (writeLog.getEndIndex() <= index) { // compare again synchronized
        return false;
      }
      q.offer(writeLog);
      return true;
    }

    synchronized void updateIndex(long i) {
      index = i;

      for(;;) {
        final Task peeked = q.peek();
        if (peeked == null || peeked.getEndIndex() > index) {
          return;
        }
        final Task polled = q.poll();
        Preconditions.assertTrue(polled == peeked);
        polled.completeFuture();
      }
    }
  }

  private final Consumer<Object> infoIndexChange = s -> LOG.info("{}: {}", this, s);
  private final Consumer<Object> traceIndexChange = s -> LOG.trace("{}: {}", this, s);

  private final String name;
  /**
   * The task queue accessed by rpc handler threads and the io worker thread.
   */
  private final DataBlockingQueue<Task> queue;
  private final WriteLogTasks writeTasks = new WriteLogTasks();
  private volatile boolean running = true;
  private final Thread workerThread;

  private final RaftStorage storage;
  private volatile SegmentedRaftLogOutputStream out;
  private final Runnable submitUpdateCommitEvent;
  private final StateMachine stateMachine;
  private final Timer logFlushTimer;
  private final Timer raftLogSyncTimer;
  private final Timer raftLogQueueingTimer;
  private final Timer raftLogEnqueueingDelayTimer;
  private final RaftLogMetrics raftLogMetrics;
  private final ByteBuffer writeBuffer;

  /**
   * The number of entries that have been written into the SegmentedRaftLogOutputStream but
   * has not been flushed.
   */
  private int pendingFlushNum = 0;
  /** the index of the last entry that has been written */
  private long lastWrittenIndex;
  /** the largest index of the entry that has been flushed */
  private final RaftLogIndex flushIndex = new RaftLogIndex("flushIndex", 0);
  /** the largest index of the entry in a closed segment */
  /** the index up to which cache can be evicted - max of snapshotIndex and
   * largest index in a closed segment */
  private final RaftLogIndex safeCacheEvictIndex = new RaftLogIndex("safeCacheEvictIndex", 0);

  private final int forceSyncNum;

  private final long segmentMaxSize;
  private final long preallocatedSize;
  private final int bufferSize;
  private final RaftServerImpl server;
  private int flushBatchSize;

  private final StateMachineDataPolicy stateMachineDataPolicy;

  SegmentedRaftLogWorker(RaftGroupMemberId memberId, StateMachine stateMachine, Runnable submitUpdateCommitEvent,
                         RaftServerImpl server, RaftStorage storage, RaftProperties properties,
                         RaftLogMetrics metricRegistry) {
    this.name = memberId + "-" + getClass().getSimpleName();
    LOG.info("new {} for {}", name, storage);

    this.submitUpdateCommitEvent = submitUpdateCommitEvent;
    this.stateMachine = stateMachine;
    this.raftLogMetrics = metricRegistry;
    this.storage = storage;
    this.server = server;
    final SizeInBytes queueByteLimit = RaftServerConfigKeys.Log.queueByteLimit(properties);
    final int queueElementLimit = RaftServerConfigKeys.Log.queueElementLimit(properties);
    this.queue =
        new DataBlockingQueue<>(name, queueByteLimit, queueElementLimit, Task::getSerializedSize);

    this.segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize = RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize = RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
    this.forceSyncNum = RaftServerConfigKeys.Log.forceSyncNum(properties);
    this.flushBatchSize = 0;

    this.stateMachineDataPolicy = new StateMachineDataPolicy(properties);

    this.workerThread = new Thread(this, name);

    // Server Id can be null in unit tests
    metricRegistry.addDataQueueSizeGauge(queue);
    metricRegistry.addLogWorkerQueueSizeGauge(writeTasks.q);
    metricRegistry.addFlushBatchSizeGauge(() -> (Gauge<Integer>) () -> flushBatchSize);
    this.logFlushTimer = metricRegistry.getFlushTimer();
    this.raftLogSyncTimer = metricRegistry.getRaftLogSyncTimer();
    this.raftLogQueueingTimer = metricRegistry.getRaftLogQueueTimer();
    this.raftLogEnqueueingDelayTimer = metricRegistry.getRaftLogEnqueueDelayTimer();

    this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
  }

  void start(long latestIndex, long evictIndex, File openSegmentFile) throws IOException {
    LOG.trace("{} start(latestIndex={}, openSegmentFile={})", name, latestIndex, openSegmentFile);
    lastWrittenIndex = latestIndex;
    flushIndex.setUnconditionally(latestIndex, infoIndexChange);
    safeCacheEvictIndex.setUnconditionally(evictIndex, infoIndexChange);
    if (openSegmentFile != null) {
      Preconditions.assertTrue(openSegmentFile.exists());
      allocateSegmentedRaftLogOutputStream(openSegmentFile, true);
    }
    workerThread.start();
  }

  void close() {
    this.running = false;
    workerThread.interrupt();
    try {
      workerThread.join(3000);
    } catch (InterruptedException ignored) {
    }
    IOUtils.cleanup(LOG, out);
    LOG.info("{} close()", name);
  }

  /**
   * A snapshot has just been installed on the follower. Need to update the IO
   * worker's state accordingly.
   */
  void syncWithSnapshot(long lastSnapshotIndex) {
    queue.clear();
    lastWrittenIndex = lastSnapshotIndex;
    flushIndex.setUnconditionally(lastSnapshotIndex, infoIndexChange);
    safeCacheEvictIndex.setUnconditionally(lastSnapshotIndex, infoIndexChange);
    pendingFlushNum = 0;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * This is protected by the RaftServer and RaftLog's lock.
   */
  private Task addIOTask(Task task) {
    LOG.debug("{} adds IO task {}", name, task);
    try {
      final Timer.Context enqueueTimerContext = raftLogEnqueueingDelayTimer.time();
      for(; !queue.offer(task, ONE_SECOND); ) {
        Preconditions.assertTrue(isAlive(),
            "the worker thread is not alive");
      }
      enqueueTimerContext.stop();
      task.startTimerOnEnqueue(raftLogQueueingTimer);
    } catch (Throwable t) {
      if (t instanceof InterruptedException && !running) {
        LOG.info("Got InterruptedException when adding task " + task
            + ". The SegmentedRaftLogWorker already stopped.");
      } else {
        LOG.error("Failed to add IO task {}", task, t);
        if (server != null) {
          server.shutdown();
        }
      }
    }
    return task;
  }

  boolean isAlive() {
    return running && workerThread.isAlive();
  }

  @Override
  public void run() {

    // if and when a log task encounters an exception
    RaftLogIOException logIOException = null;

    while (running) {
      try {
        Task task = queue.poll(ONE_SECOND);
        if (task != null) {
          task.stopTimerOnDequeue();
          try {
            if (logIOException != null) {
              throw logIOException;
            } else {
              Timer.Context executionTimeContext =
                  raftLogMetrics.getRaftLogTaskExecutionTimer(task.getClass().getSimpleName().toLowerCase()).time();
              task.execute();
              executionTimeContext.stop();
            }
          } catch (IOException e) {
            if (task.getEndIndex() < lastWrittenIndex) {
              LOG.info("Ignore IOException when handling task " + task
                  + " which is smaller than the lastWrittenIndex."
                  + " There should be a snapshot installed.", e);
            } else {
              task.failed(e);
              if (logIOException == null) {
                logIOException = new RaftLogIOException("Log already failed"
                    + " at index " + task.getEndIndex()
                    + " for task " + task, e);
              }
              continue;
            }
          }
          task.done();
        }
      } catch (InterruptedException e) {
        if (running) {
          LOG.warn("{} got interrupted while still running",
              Thread.currentThread().getName());
        }
        LOG.info(Thread.currentThread().getName()
            + " was interrupted, exiting. There are " + queue.getNumElements()
            + " tasks remaining in the queue.");
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable t) {
        if (!running) {
          LOG.info("{} got closed and hit exception",
              Thread.currentThread().getName(), t);
        } else {
          LOG.error("{} hit exception", Thread.currentThread().getName(), t);
          // Shutdown raft group instead of terminating jvm.
          if (server != null) {
            server.shutdown();
          }
        }
      }
    }
  }

  private boolean shouldFlush() {
    return pendingFlushNum >= forceSyncNum ||
        (pendingFlushNum > 0 && queue.isEmpty());
  }

  private void flushWrites() throws IOException {
    if (out != null) {
      LOG.debug("{}: flush {}", name, out);
      final Timer.Context timerContext = logFlushTimer.time();
      try {
        final CompletableFuture<Void> f = stateMachine != null ?
            stateMachine.data().flush(lastWrittenIndex) :
            CompletableFuture.completedFuture(null);
        if (stateMachineDataPolicy.isSync()) {
          stateMachineDataPolicy.getFromFuture(f, () -> this + "-flushStateMachineData");
        }
        final Timer.Context logSyncTimerContext = raftLogSyncTimer.time();
        flushBatchSize = (int)(lastWrittenIndex - flushIndex.get());
        out.flush();
        logSyncTimerContext.stop();
        if (!stateMachineDataPolicy.isSync()) {
          IOUtils.getFromFuture(f, () -> this + "-flushStateMachineData");
        }
      } finally {
        timerContext.stop();
      }
      updateFlushedIndexIncreasingly();
    }
  }

  private void updateFlushedIndexIncreasingly() {
    final long i = lastWrittenIndex;
    flushIndex.updateIncreasingly(i, traceIndexChange);
    postUpdateFlushedIndex();
    writeTasks.updateIndex(i);
  }

  private void postUpdateFlushedIndex() {
    pendingFlushNum = 0;
    Optional.ofNullable(submitUpdateCommitEvent).ifPresent(Runnable::run);
  }

  /**
   * The following several methods (startLogSegment, rollLogSegment,
   * writeLogEntry, and truncate) are only called by SegmentedRaftLog which is
   * protected by RaftServer's lock.
   *
   * Thus all the tasks are created and added sequentially.
   */
  void startLogSegment(long startIndex) {
    LOG.info("{}: Starting segment from index:{}", name, startIndex);
    addIOTask(new StartLogSegment(startIndex));
  }

  void rollLogSegment(LogSegment segmentToClose) {
    LOG.info("{}: Rolling segment {} to index:{}", name,
        segmentToClose.toString(), segmentToClose.getEndIndex());
    addIOTask(new FinalizeLogSegment(segmentToClose));
    addIOTask(new StartLogSegment(segmentToClose.getEndIndex() + 1));
  }

  Task writeLogEntry(LogEntryProto entry) {
    return addIOTask(new WriteLog(entry));
  }

  Task truncate(TruncationSegments ts, long index) {
    LOG.info("{}: Truncating segments {}, start index {}", name, ts, index);
    return addIOTask(new TruncateLog(ts, index));
  }

  void closeLogSegment(LogSegment segmentToClose) {
    LOG.info("{}: Closing segment {} to index: {}", name,
        segmentToClose.toString(), segmentToClose.getEndIndex());
    addIOTask(new FinalizeLogSegment(segmentToClose));
  }

  Task purge(TruncationSegments ts) {
    return addIOTask(new PurgeLog(ts, storage));
  }

  private final class PurgeLog extends Task {
    private final TruncationSegments segments;
    private final RaftStorage storage;

    private PurgeLog(TruncationSegments segments, RaftStorage storage) {
      this.segments = segments;
      this.storage = storage;
    }

    @Override
    void execute() throws IOException {
      if (segments.getToDelete() != null) {
        Timer.Context purgeLogContext = raftLogMetrics.getRaftLogPurgeTimer().time();
        for (SegmentFileInfo fileInfo : segments.getToDelete()) {
          File delFile = storage.getStorageDir()
                  .getClosedLogFile(fileInfo.getStartIndex(), fileInfo.getEndIndex());
          FileUtils.deleteFile(delFile);
        }
        purgeLogContext.stop();
      }
    }

    @Override
    long getEndIndex() {
      return segments.maxEndIndex();
    }
  }

  private class WriteLog extends Task {
    private final LogEntryProto entry;
    private final CompletableFuture<?> stateMachineFuture;
    private final CompletableFuture<Long> combined;

    WriteLog(LogEntryProto entry) {
      this.entry = ServerProtoUtils.removeStateMachineData(entry);
      if (this.entry == entry || stateMachine == null) {
        this.stateMachineFuture = null;
      } else {
        try {
          // this.entry != entry iff the entry has state machine data
          this.stateMachineFuture = stateMachine.data().write(entry);
        } catch (Throwable e) {
          LOG.error(name + ": writeStateMachineData failed for index " + entry.getIndex()
              + ", entry=" + ServerProtoUtils.toLogEntryString(entry, stateMachine::toStateMachineLogEntryString), e);
          throw e;
        }
      }
      this.combined = stateMachineFuture == null? super.getFuture()
          : super.getFuture().thenCombine(stateMachineFuture, (index, stateMachineResult) -> index);
    }

    @Override
    void failed(IOException e) {
      stateMachine.notifyLogFailed(e, entry);
      super.failed(e);
    }

    @Override
    int getSerializedSize() {
      return ServerProtoUtils.getSerializedSize(entry);
    }

    @Override
    CompletableFuture<Long> getFuture() {
      return combined;
    }

    @Override
    void done() {
      writeTasks.offerOrCompleteFuture(this);
    }

    @Override
    public void execute() throws IOException {
      if (stateMachineDataPolicy.isSync() && stateMachineFuture != null) {
        stateMachineDataPolicy.getFromFuture(stateMachineFuture, () -> this + "-writeStateMachineData");
      }

      raftLogMetrics.onRaftLogAppendEntry();
      Preconditions.assertTrue(out != null);
      Preconditions.assertTrue(lastWrittenIndex + 1 == entry.getIndex(),
          "lastWrittenIndex == %s, entry == %s", lastWrittenIndex, entry);
      out.write(entry);
      lastWrittenIndex = entry.getIndex();
      pendingFlushNum++;
      if (shouldFlush()) {
        raftLogMetrics.onRaftLogFlush();
        flushWrites();
      }
    }

    @Override
    long getEndIndex() {
      return entry.getIndex();
    }

    @Override
    public String toString() {
      return super.toString() + ": " + ServerProtoUtils.toLogEntryString(
          entry, stateMachine == null? null: stateMachine::toStateMachineLogEntryString);
    }
  }

  private class FinalizeLogSegment extends Task {
    private final long startIndex;
    private final long endIndex;

    FinalizeLogSegment(LogSegment segmentToClose) {
      Preconditions.assertTrue(segmentToClose != null, "Log segment to be rolled is null");
      this.startIndex = segmentToClose.getStartIndex();
      this.endIndex = segmentToClose.getEndIndex();
    }

    @Override
    public void execute() throws IOException {
      freeSegmentedRaftLogOutputStream();

      File openFile = storage.getStorageDir().getOpenLogFile(startIndex);
      Preconditions.assertTrue(openFile.exists(),
          () -> name + ": File " + openFile + " to be rolled does not exist");
      if (endIndex - startIndex + 1 > 0) {
        // finalize the current open segment
        File dstFile = storage.getStorageDir().getClosedLogFile(startIndex, endIndex);
        Preconditions.assertTrue(!dstFile.exists());

        FileUtils.move(openFile, dstFile);
        LOG.info("{}: Rolled log segment from {} to {}", name, openFile, dstFile);
      } else { // delete the file of the empty segment
        FileUtils.deleteFile(openFile);
        LOG.info("{}: Deleted empty log segment {}", name, openFile);
      }
      updateFlushedIndexIncreasingly();
      safeCacheEvictIndex.updateToMax(endIndex, traceIndexChange);
    }

    @Override
    void failed(IOException e) {
      // not failed for a specific log entry, but an entire segment
      stateMachine.notifyLogFailed(e, null);
      super.failed(e);
    }

    @Override
    long getEndIndex() {
      return endIndex;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + "startIndex=" + startIndex + " endIndex=" + endIndex;
    }
  }

  private class StartLogSegment extends Task {
    private final long newStartIndex;

    StartLogSegment(long newStartIndex) {
      this.newStartIndex = newStartIndex;
    }

    @Override
    void execute() throws IOException {
      File openFile = storage.getStorageDir().getOpenLogFile(newStartIndex);
      Preconditions.assertTrue(!openFile.exists(), "open file %s exists for %s",
          openFile, name);
      Preconditions.assertTrue(pendingFlushNum == 0);
      allocateSegmentedRaftLogOutputStream(openFile, false);
      Preconditions.assertTrue(openFile.exists(), "Failed to create file %s for %s",
          openFile.getAbsolutePath(), name);
      LOG.info("{}: created new log segment {}", name, openFile);
    }

    @Override
    long getEndIndex() {
      return newStartIndex;
    }
  }

  private class TruncateLog extends Task {
    private final TruncationSegments segments;
    private final long truncateIndex;
    private CompletableFuture<Void> stateMachineFuture = null;

    TruncateLog(TruncationSegments ts, long index) {
      this.segments = ts;
      this.truncateIndex = index;
      if (stateMachine != null) {
        // TruncateLog and WriteLog instance is created while taking a RaftLog write lock.
        // StateMachine call is made inside the constructor so that it is lock
        // protected. This is to make sure that stateMachine can determine which
        // indexes to truncate as stateMachine calls would happen in the sequence
        // of log operations.
        stateMachineFuture = stateMachine.data().truncate(truncateIndex);
      }
    }

    @Override
    void execute() throws IOException {
      freeSegmentedRaftLogOutputStream();

      if (segments.getToTruncate() != null) {
        File fileToTruncate = segments.getToTruncate().isOpen() ?
            storage.getStorageDir().getOpenLogFile(
                segments.getToTruncate().getStartIndex()) :
            storage.getStorageDir().getClosedLogFile(
                segments.getToTruncate().getStartIndex(),
                segments.getToTruncate().getEndIndex());
        Preconditions.assertTrue(fileToTruncate.exists(),
            "File %s to be truncated does not exist", fileToTruncate);
        FileUtils.truncateFile(fileToTruncate, segments.getToTruncate().getTargetLength());

        // rename the file
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segments.getToTruncate().getStartIndex(), segments.getToTruncate().getNewEndIndex());
        Preconditions.assertTrue(!dstFile.exists(),
            "Truncated file %s already exists ", dstFile);
        FileUtils.move(fileToTruncate, dstFile);
        LOG.info("{}: Truncated log file {} to length {} and moved it to {}", name,
            fileToTruncate, segments.getToTruncate().getTargetLength(), dstFile);

        // update lastWrittenIndex
        lastWrittenIndex = segments.getToTruncate().getNewEndIndex();
      }
      if (segments.getToDelete() != null && segments.getToDelete().length > 0) {
        long minStart = segments.getToDelete()[0].getStartIndex();
        for (SegmentFileInfo del : segments.getToDelete()) {
          final File delFile;
          if (del.isOpen()) {
            delFile = storage.getStorageDir().getOpenLogFile(del.getStartIndex());
          } else {
            delFile = storage.getStorageDir()
                .getClosedLogFile(del.getStartIndex(), del.getEndIndex());
          }
          Preconditions.assertTrue(delFile.exists(),
              "File %s to be deleted does not exist", delFile);
          FileUtils.deleteFile(delFile);
          LOG.info("{}: Deleted log file {}", name, delFile);
          minStart = Math.min(minStart, del.getStartIndex());
        }
        if (segments.getToTruncate() == null) {
          lastWrittenIndex = minStart - 1;
        }
      }
      if (stateMachineFuture != null) {
        IOUtils.getFromFuture(stateMachineFuture, () -> this + "-truncateStateMachineData");
      }
      flushIndex.setUnconditionally(lastWrittenIndex, infoIndexChange);
      safeCacheEvictIndex.setUnconditionally(lastWrittenIndex, infoIndexChange);
      postUpdateFlushedIndex();
    }

    @Override
    long getEndIndex() {
      if (segments.getToTruncate() != null) {
        return segments.getToTruncate().getNewEndIndex();
      } else if (segments.getToDelete().length > 0) {
        return segments.getToDelete()[segments.getToDelete().length - 1].getEndIndex();
      }
      return RaftLog.INVALID_LOG_INDEX;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + segments;
    }
  }

  long getFlushIndex() {
    return flushIndex.get();
  }

  long getSafeCacheEvictIndex() {
    return safeCacheEvictIndex.get();
  }

  private void freeSegmentedRaftLogOutputStream() {
    IOUtils.cleanup(LOG, out);
    out = null;
    Preconditions.assertTrue(writeBuffer.position() == 0);
  }

  private void allocateSegmentedRaftLogOutputStream(File file, boolean append) throws IOException {
    Preconditions.assertTrue(out == null && writeBuffer.position() == 0);
    out = new SegmentedRaftLogOutputStream(file, append, segmentMaxSize,
            preallocatedSize, writeBuffer);
  }
}
