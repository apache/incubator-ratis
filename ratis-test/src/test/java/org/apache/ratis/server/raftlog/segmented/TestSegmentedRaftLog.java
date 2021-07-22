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

import static org.junit.Assert.assertTrue;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.metrics.RaftLogMetricsBase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.codahale.metrics.Timer;

public class TestSegmentedRaftLog extends BaseTest {
  static {
    Log4jUtils.setLogLevel(SegmentedRaftLogWorker.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(SegmentedRaftLogCache.LOG, Level.TRACE);
    Log4jUtils.setLogLevel(SegmentedRaftLog.LOG, Level.TRACE);
  }

  public static long getOpenSegmentSize(RaftLog raftLog) {
    return ((SegmentedRaftLog)raftLog).getRaftLogCache().getOpenSegment().getTotalFileSize();
  }

  private static final RaftPeerId peerId = RaftPeerId.valueOf("s0");
  private static final RaftGroupId groupId = RaftGroupId.randomId();
  private static final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(peerId, groupId);

  static class SegmentRange {
    final long start;
    final long end;
    final long term;
    final boolean isOpen;

    SegmentRange(long s, long e, long term, boolean isOpen) {
      this.start = s;
      this.end = e;
      this.term = term;
      this.isOpen = isOpen;
    }

    File getFile(RaftStorage storage) {
      return LogSegmentStartEnd.valueOf(start, end, isOpen).getFile(storage);
    }
  }

  private File storageDir;
  private RaftProperties properties;
  private RaftStorage storage;
  private long segmentMaxSize;
  private long preallocatedSize;
  private int bufferSize;

  SegmentedRaftLog newSegmentedRaftLog() {
    return newSegmentedRaftLog(storage, properties);
  }

  SegmentedRaftLog newSegmentedRaftLog(LongSupplier getSnapshotIndexFromStateMachine) {
    return newSegmentedRaftLogWithSnapshotIndex(storage, properties, getSnapshotIndexFromStateMachine);
  }

  static SegmentedRaftLog newSegmentedRaftLog(RaftStorage storage, RaftProperties properties) {
    return new SegmentedRaftLog(memberId, null, null, null, null, storage,
        () -> -1, properties);
  }

  private SegmentedRaftLog newSegmentedRaftLogWithSnapshotIndex(RaftStorage storage, RaftProperties properties,
                                                                LongSupplier getSnapshotIndexFromStateMachine) {
    return new SegmentedRaftLog(memberId, null, null, null, null, storage,
        getSnapshotIndexFromStateMachine, properties);
  }

  @Before
  public void setup() throws Exception {
    storageDir = getTestDir();
    properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties,  Collections.singletonList(storageDir));
    storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    this.segmentMaxSize =
        RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize =
        RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize =
        RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] prepareLog(List<SegmentRange> list) throws IOException {
    List<LogEntryProto> entryList = new ArrayList<>();
    for (SegmentRange range : list) {
      final File file = range.getFile(storage);
      final int size = (int) (range.end - range.start + 1);
      LogEntryProto[] entries = new LogEntryProto[size];
      try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(file, false,
          segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
        for (int i = 0; i < size; i++) {
          SimpleOperation m = new SimpleOperation("m" + (i + range.start));
          entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), range.term, i + range.start);
          out.write(entries[i]);
        }
      }
      Collections.addAll(entryList, entries);
    }
    return entryList.toArray(new LogEntryProto[entryList.size()]);
  }

  static List<SegmentRange> prepareRanges(int startTerm, int endTerm, int segmentSize,
      long startIndex) {
    List<SegmentRange> list = new ArrayList<>(endTerm - startTerm);
    for (int i = startTerm; i < endTerm; i++) {
      list.add(new SegmentRange(startIndex, startIndex + segmentSize - 1, i,
          i == endTerm - 1));
      startIndex += segmentSize;
    }
    return list;
  }

  private LogEntryProto getLastEntry(SegmentedRaftLog raftLog)
      throws IOException {
    return raftLog.get(raftLog.getLastEntryTermIndex().getIndex());
  }

  @Test
  public void testLoadLogSegments() throws Exception {
    // first generate log files
    List<SegmentRange> ranges = prepareRanges(0, 5, 100, 0);
    LogEntryProto[] entries = prepareLog(ranges);

    // create RaftLog object and load log file
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if log entries are loaded correctly
      for (LogEntryProto e : entries) {
        LogEntryProto entry = raftLog.get(e.getIndex());
        Assert.assertEquals(e, entry);
      }

      final LogEntryHeader[] termIndices = raftLog.getEntries(0, 500);
      LogEntryProto[] entriesFromLog = Arrays.stream(termIndices)
          .map(ti -> {
            try {
              return raftLog.get(ti.getIndex());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(LogEntryProto[]::new);
      Assert.assertArrayEquals(entries, entriesFromLog);
      Assert.assertEquals(entries[entries.length - 1], getLastEntry(raftLog));

      final RatisMetricRegistry metricRegistryForLogWorker = RaftLogMetricsBase.getLogWorkerMetricRegistry(memberId);

      Timer raftLogSegmentLoadLatencyTimer = metricRegistryForLogWorker.timer("segmentLoadLatency");
      assertTrue(raftLogSegmentLoadLatencyTimer.getMeanRate() > 0);

      Timer raftLogReadLatencyTimer = metricRegistryForLogWorker.timer("readEntryLatency");
      assertTrue(raftLogReadLatencyTimer.getMeanRate() > 0);
    }
  }

  static List<LogEntryProto> prepareLogEntries(List<SegmentRange> slist,
      Supplier<String> stringSupplier) {
    List<LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      prepareLogEntries(range, stringSupplier, false, eList);
    }
    return eList;
  }

  static List<LogEntryProto> prepareLogEntries(SegmentRange range,
      Supplier<String> stringSupplier, boolean hasStataMachineData, List<LogEntryProto> eList) {
    for(long index = range.start; index <= range.end; index++) {
      eList.add(prepareLogEntry(range.term, index, stringSupplier, hasStataMachineData));
    }
    return eList;
  }

  static LogEntryProto prepareLogEntry(long term, long index, Supplier<String> stringSupplier, boolean hasStataMachineData) {
    final SimpleOperation m = stringSupplier == null?
        new SimpleOperation("m" + index, hasStataMachineData):
        new SimpleOperation(stringSupplier.get(), hasStataMachineData);
    return LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), term, index);
  }

  /**
   * Append entry one by one and check if log state is correct.
   */
  @Test
  public void testAppendEntry() throws Exception {
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      checkEntries(raftLog, entries, 0, entries.size());
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      TermIndex lastTermIndex  = raftLog.getLastEntryTermIndex();
      IllegalStateException ex = null;
      try {
        // append entry fails if append entry term is lower than log's last entry term
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm() - 1)
            .setIndex(lastTermIndex.getIndex() + 1).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("term less than RaftLog's last term"));
      try {
        // append entry fails if difference between append entry index and log's last entry index is greater than 1
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm())
            .setIndex(lastTermIndex.getIndex() + 2).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("and RaftLog's last index " + lastTermIndex.getIndex()
          + " (or snapshot index " + raftLog.getSnapshotIndex() + ") is greater than 1"));

      raftLog.onSnapshotInstalled(raftLog.getLastEntryTermIndex().getIndex());
      try {
        // append entry fails if there are no log entries && log's snapshotIndex + 1 < incoming log entry.
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm())
            .setIndex(lastTermIndex.getIndex() + 2).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("Difference between entry index and RaftLog's latest snapshot " +
          "index -1 is greater than 1"));
    }
  }

  @Test
  public void testAppendEntryAfterPurge() throws Exception {
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    long desiredSnapshotIndex = entries.size() - 2;
    final LongSupplier getSnapshotIndexFromStateMachine = new LongSupplier() {
      private boolean firstCall = true;
      @Override
      public long getAsLong() {
        long index = firstCall ? -1 : desiredSnapshotIndex;
        firstCall = !firstCall;
        return index;
      }
    };

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog(getSnapshotIndexFromStateMachine)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.subList(0, entries.size() - 1).stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);

      raftLog.onSnapshotInstalled(desiredSnapshotIndex);
      // Try appending last entry after snapshot + purge.
      CompletableFuture<Long> appendEntryFuture =
          raftLog.appendEntry(entries.get(entries.size() - 1));
      assertTrue(desiredSnapshotIndex + 1 == appendEntryFuture.get());
    }
  }

  /**
   * Keep appending entries, make sure the rolling is correct.
   */
  @Test
  public void testAppendAndRoll() throws Exception {
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf("16KB"));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("128KB"));

    List<SegmentRange> ranges = prepareRanges(0, 1, 1024, 0);
    final byte[] content = new byte[1024];
    List<LogEntryProto> entries = prepareLogEntries(ranges,
        () -> new String(content));

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      checkEntries(raftLog, entries, 0, entries.size());
      Assert.assertEquals(9, raftLog.getRaftLogCache().getNumOfSegments());
    }
  }

  @Test
  public void testTruncate() throws Exception {
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    for (long fromIndex = 900; fromIndex >= 0; fromIndex -= 150) {
      testTruncate(entries, fromIndex);
    }
  }

  private void testTruncate(List<LogEntryProto> entries, long fromIndex)
      throws Exception {
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // truncate the log
      raftLog.truncate(fromIndex).join();


      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      if (fromIndex > 0) {
        Assert.assertEquals(entries.get((int) (fromIndex - 1)),
            getLastEntry(raftLog));
      } else {
        Assert.assertNull(raftLog.getLastEntryTermIndex());
      }
      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }
  }

  private void checkEntries(RaftLog raftLog, List<LogEntryProto> expected,
      int offset, int size) throws IOException {
    if (size > 0) {
      for (int i = offset; i < size + offset; i++) {
        LogEntryProto entry = raftLog.get(expected.get(i).getIndex());
        Assert.assertEquals(expected.get(i), entry);
      }
      final LogEntryHeader[] termIndices = raftLog.getEntries(
          expected.get(offset).getIndex(),
          expected.get(offset + size - 1).getIndex() + 1);
      LogEntryProto[] entriesFromLog = Arrays.stream(termIndices)
          .map(ti -> {
            try {
              return raftLog.get(ti.getIndex());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(LogEntryProto[]::new);
      LogEntryProto[] expectedArray = expected.subList(offset, offset + size)
          .stream().toArray(LogEntryProto[]::new);
      Assert.assertArrayEquals(expectedArray, entriesFromLog);
    }
  }

  private void checkFailedEntries(List<LogEntryProto> entries, long fromIndex, RetryCache retryCache) {
    for (int i = 0; i < entries.size(); i++) {
      if (i < fromIndex) {
        RetryCacheTestUtil.assertFailure(retryCache, entries.get(i), false);
      } else {
        RetryCacheTestUtil.assertFailure(retryCache, entries.get(i), true);
      }
    }
  }

  @Test
  public void testPurgeOnOpenSegment() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long beginIndexOfOpenSegment = segmentSize * (endTerm - startTerm - 1);
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, beginIndexOfOpenSegment, expectedIndex);
  }

  @Test
  public void testPurgeOnClosedSegments() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, endIndexOfClosedSegment, expectedIndex);
  }

  @Test
  public void testPurgeLogMetric() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    final RatisMetricRegistry metricRegistryForLogWorker = RaftLogMetricsBase.getLogWorkerMetricRegistry(memberId);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, endIndexOfClosedSegment, expectedIndex);
    assertTrue(metricRegistryForLogWorker.timer("purgeLog").getCount() > 0);
  }

  @Test
  public void testPurgeOnClosedSegmentsWithPurgeGap() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = RaftLog.LEAST_VALID_LOG_INDEX;
    purgeAndVerify(startTerm, endTerm, segmentSize, 1000, endIndexOfClosedSegment, expectedIndex);
  }

  private void purgeAndVerify(int startTerm, int endTerm, int segmentSize, int purgeGap, long purgeIndex, long expectedIndex) throws Exception {
    List<SegmentRange> ranges = prepareRanges(startTerm, endTerm, segmentSize, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    final RaftProperties p = new RaftProperties();
    RaftServerConfigKeys.Log.setPurgeGap(p, purgeGap);
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog(storage, p)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
      final CompletableFuture<Long> f = raftLog.purge(purgeIndex);
      final Long purged = f.get();
      LOG.info("purgeIndex = {}, purged = {}", purgeIndex, purged);
      Assert.assertEquals(expectedIndex, raftLog.getRaftLogCache().getStartIndex());
    }
  }

  /**
   * Test append with inconsistent entries
   */
  @Test
  public void testAppendEntriesWithInconsistency() throws Exception {
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    final RetryCache retryCache = RetryCacheTestUtil.createRetryCache();
    try (SegmentedRaftLog raftLog = RetryCacheTestUtil.newSegmentedRaftLog(memberId, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.forEach(entry -> RetryCacheTestUtil.createEntry(retryCache, entry));
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    // append entries whose first 100 entries are the same with existing log,
    // and the next 100 are with different term
    SegmentRange r1 = new SegmentRange(550, 599, 2, false);
    SegmentRange r2 = new SegmentRange(600, 649, 3, false);
    SegmentRange r3 = new SegmentRange(650, 749, 10, false);
    List<LogEntryProto> newEntries = prepareLogEntries(
        Arrays.asList(r1, r2, r3), null);

    try (SegmentedRaftLog raftLog = RetryCacheTestUtil.newSegmentedRaftLog(memberId, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      LOG.info("newEntries[0] = {}", newEntries.get(0));
      final int last = newEntries.size() - 1;
      LOG.info("newEntries[{}] = {}", last, newEntries.get(last));
      raftLog.append(newEntries.toArray(new LogEntryProto[0])).forEach(CompletableFuture::join);

      checkFailedEntries(entries, 650, retryCache);
      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assert.assertEquals(newEntries.get(newEntries.size() - 1),
          getLastEntry(raftLog));
      Assert.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getFlushIndex());
    }

    // load the raftlog again and check
    try (SegmentedRaftLog raftLog = RetryCacheTestUtil.newSegmentedRaftLog(memberId, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assert.assertEquals(newEntries.get(newEntries.size() - 1),
          getLastEntry(raftLog));
      Assert.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getFlushIndex());

      SegmentedRaftLogCache cache = raftLog.getRaftLogCache();
      Assert.assertEquals(5, cache.getNumOfSegments());
    }
  }

  @Test
  public void testSegmentedRaftLogStateMachineData() throws Exception {
    final SegmentRange range = new SegmentRange(0, 10, 1, true);
    final List<LogEntryProto> entries = prepareLogEntries(range, null, true, new ArrayList<>());

    final SimpleStateMachine4Testing sm = new SimpleStateMachine4Testing();
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(memberId, null, sm, null, null, storage, () -> -1, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);

      int next = 0;
      long flush = -1;
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndicesMultipleAttempts(raftLog, flush += 3, next);

      sm.blockFlushStateMachineData();
      raftLog.appendEntry(entries.get(next++));

      sm.blockWriteStateMachineData();
      final Thread t = startAppendEntryThread(raftLog, entries.get(next++));
      TimeUnit.SECONDS.sleep(1);
      assertTrue(t.isAlive());
      sm.unblockWriteStateMachineData();

      assertIndices(raftLog, flush, next);
      TimeUnit.SECONDS.sleep(1);
      assertIndices(raftLog, flush, next);
      sm.unblockFlushStateMachineData();
      assertIndicesMultipleAttempts(raftLog, flush + 2, next);

      // raftLog.appendEntry(entry).get() won't return
      // until sm.unblockFlushStateMachineData() was called.
      t.join();
    }
  }

  @Test(expected = TimeoutIOException.class)
  public void testServerShutdownOnTimeoutIOException() throws Throwable {
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
    final TimeDuration syncTimeout = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeout(properties, syncTimeout);
    final int numRetries = 2;
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(properties, numRetries);

    final LogEntryProto entry = prepareLogEntry(0, 0, null, true);
    final StateMachine sm = new BaseStateMachine() {
      @Override
      public CompletableFuture<Void> write(LogEntryProto entry) {
        getLifeCycle().transition(LifeCycle.State.STARTING);
        getLifeCycle().transition(LifeCycle.State.RUNNING);

        return new CompletableFuture<>(); // the future never completes
      }

      @Override
      public void notifyLogFailed(Throwable cause, LogEntryProto entry) {
        LOG.info("Test StateMachine: Ratis log failed notification received as expected.", cause);

        LOG.info("Test StateMachine: Transition to PAUSED state.");
        Assert.assertNotNull(entry);

        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
      }
    };

    Throwable ex = null; // TimeoutIOException
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(memberId, null, sm, null, null, storage, () -> -1, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // SegmentedRaftLogWorker should catch TimeoutIOException
      CompletableFuture<Long> f = raftLog.appendEntry(entry);
      // Wait for async writeStateMachineData to finish
      try {
        f.get();
      } catch (ExecutionException e) {
        ex = e.getCause();
      }
    }
    Assert.assertNotNull(ex);
    Assert.assertSame(LifeCycle.State.PAUSED, sm.getLifeCycleState());
    throw ex;
  }

  /**
   * Verifies that getOrCreateEntry function creates cache entry in every case and does not return null.
   */
  @Test
  public void testGetOrCreateEntry() {
    final RetryCache retryCache = RetryCacheTestUtil.createRetryCache();
    final ClientId clientId = ClientId.randomId();
    final long invocationId1 = 123456789;
    final ClientInvocationId clientInvocationId1 = ClientInvocationId.valueOf(clientId, invocationId1);
    RetryCache.Entry cacheEntry1 = RetryCacheTestUtil.getOrCreateEntry(retryCache, clientInvocationId1);
    Assert.assertNotNull(cacheEntry1);

    RetryCache.Entry cacheEntry1Again = RetryCacheTestUtil.getOrCreateEntry(retryCache, clientInvocationId1);
    Assert.assertEquals(cacheEntry1.toString(), cacheEntry1Again.toString());

    final long invocationId2 = 987654321;
    final ClientInvocationId clientInvocationId2 = ClientInvocationId.valueOf(clientId, invocationId2);
    RetryCache.Entry cacheEntry2 = RetryCacheTestUtil.getOrCreateEntry(retryCache, clientInvocationId2);
    Assert.assertNotNull(cacheEntry2);

    Assert.assertNotEquals(cacheEntry1.toString(), cacheEntry2.toString());
  }

  static Thread startAppendEntryThread(RaftLog raftLog, LogEntryProto entry) {
    final Thread t = new Thread(() -> {
      try {
        raftLog.appendEntry(entry).get();
      } catch (Throwable e) {
        // just ignore
      }
    });
    t.start();
    return t;
  }

  void assertIndices(RaftLog raftLog, long expectedFlushIndex, long expectedNextIndex) {
    LOG.info("assert expectedFlushIndex={}", expectedFlushIndex);
    Assert.assertEquals(expectedFlushIndex, raftLog.getFlushIndex());
    LOG.info("assert expectedNextIndex={}", expectedNextIndex);
    Assert.assertEquals(expectedNextIndex, raftLog.getNextIndex());
  }

  void assertIndicesMultipleAttempts(RaftLog raftLog, long expectedFlushIndex, long expectedNextIndex) throws Exception {
    JavaUtils.attempt(() -> assertIndices(raftLog, expectedFlushIndex, expectedNextIndex),
        10, HUNDRED_MILLIS, "assertIndices", LOG);
  }

  @Test
  public void testSegmentedRaftLogFormatInternalHeader() throws Exception {
    testFailureCase("testSegmentedRaftLogFormatInternalHeader",
        () -> SegmentedRaftLogFormat.applyHeaderTo(header -> {
          LOG.info("header  = " + new String(header, StandardCharsets.UTF_8));
          header[0] += 1; // try changing the internal header
          LOG.info("header' = " + new String(header, StandardCharsets.UTF_8));
          return null;
        }), IllegalStateException.class);

    // reset the header
    SegmentedRaftLogFormat.applyHeaderTo(header -> {
      LOG.info("header'  = " + new String(header, StandardCharsets.UTF_8));
      header[0] -= 1; // try changing the internal header
      LOG.info("header'' = " + new String(header, StandardCharsets.UTF_8));
      return null;
    });
  }
}
