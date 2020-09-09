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
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.WatchRequestTypeProto;
import org.apache.ratis.protocol.NotReplicatedException;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ResourceSemaphore;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class WatchRequests {
  public static final Logger LOG = LoggerFactory.getLogger(WatchRequests.class);

  static class PendingWatch {
    private final WatchRequestTypeProto watch;
    private final Timestamp creationTime;
    private final Supplier<CompletableFuture<Void>> future = JavaUtils.memoize(CompletableFuture::new);

    PendingWatch(WatchRequestTypeProto watch, Timestamp creationTime) {
      this.watch = watch;
      this.creationTime = creationTime;
    }

    CompletableFuture<Void> getFuture() {
      return future.get();
    }

    long getIndex() {
      return watch.getIndex();
    }

    Timestamp getCreationTime() {
      return creationTime;
    }

    @Override
    public String toString() {
      return RaftClientRequest.Type.toString(watch) + "@" + creationTime
          + "?" + StringUtils.completableFuture2String(future.get(), true);
    }
  }

  private class WatchQueue {
    private final ReplicationLevel replication;
    private final SortedMap<PendingWatch, PendingWatch> q = new TreeMap<>(
        Comparator.comparingLong(PendingWatch::getIndex).thenComparing(PendingWatch::getCreationTime));
    private final ResourceSemaphore resource;
    private volatile long index; //Invariant: q.isEmpty() or index < any element q

    WatchQueue(ReplicationLevel replication, int elementLimit) {
      this.replication = replication;
      this.resource = new ResourceSemaphore(elementLimit);
    }

    long getIndex() {
      return index;
    }

    CompletableFuture<Void> add(RaftClientRequest request) {
      final long currentTime = Timestamp.currentTimeNanos();
      final long roundUp = watchTimeoutDenominationNanos.roundUpNanos(currentTime);
      final PendingWatch pending = new PendingWatch(request.getType().getWatch(), Timestamp.valueOf(roundUp));

      final PendingWatch computed;
      synchronized (this) {
        if (pending.getIndex() <= getIndex()) { // compare again synchronized
          // watch condition already satisfied
          return null;
        }
        computed = q.compute(pending, (key, old) -> old != null? old: resource.tryAcquire()? pending: null);
      }

      if (computed == null) {
        // failed to acquire
        return JavaUtils.completeExceptionally(new ResourceUnavailableException(
            "Failed to acquire a pending watch request in " + name + " for " + request));
      }
      if (computed != pending) {
        // already exists in q
        return computed.getFuture();
      }

      // newly added to q
      final TimeDuration timeout = watchTimeoutNanos.apply(duration -> duration + roundUp - currentTime);
      scheduler.onTimeout(timeout, () -> handleTimeout(request, pending),
          LOG, () -> name + ": Failed to timeout " + request);
      return pending.getFuture();
    }

    void handleTimeout(RaftClientRequest request, PendingWatch pending) {
      if (removeExisting(pending)) {
        pending.getFuture().completeExceptionally(
            new NotReplicatedException(request.getCallId(), replication, pending.getIndex()));
        LOG.debug("{}: timeout {}, {}", name, pending, request);
      }
    }

    synchronized boolean removeExisting(PendingWatch pending) {
      final PendingWatch removed = q.remove(pending);
      if (removed == null) {
        return false;
      }
      Preconditions.assertTrue(removed == pending);
      resource.release();
      return true;
    }

    synchronized void updateIndex(final long newIndex) {
      if (newIndex <= getIndex()) { // compare again synchronized
        return;
      }
      LOG.debug("{}: update {} index from {} to {}", name, replication, index, newIndex);
      index = newIndex;

      for(; !q.isEmpty();) {
        final PendingWatch first = q.firstKey();
        if (first.getIndex() > newIndex) {
          return;
        }
        final boolean removed = removeExisting(first);
        Preconditions.assertTrue(removed);
        LOG.debug("{}: complete {}", name, first);
        first.getFuture().complete(null);
      }
    }

    synchronized void failAll(Exception e) {
      for(PendingWatch pending : q.values()) {
        pending.getFuture().completeExceptionally(e);
      }
      q.clear();
      resource.close();
    }
  }

  private final String name;
  private final Map<ReplicationLevel, WatchQueue> queues = new EnumMap<>(ReplicationLevel.class);

  private final TimeDuration watchTimeoutNanos;
  private final TimeDuration watchTimeoutDenominationNanos;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();

  WatchRequests(Object name, RaftProperties properties) {
    this.name = name + "-" + getClass().getSimpleName();

    final TimeDuration watchTimeout = RaftServerConfigKeys.Watch.timeout(properties);
    this.watchTimeoutNanos = watchTimeout.to(TimeUnit.NANOSECONDS);
    final TimeDuration watchTimeoutDenomination = RaftServerConfigKeys.Watch.timeoutDenomination(properties);
    this.watchTimeoutDenominationNanos = watchTimeoutDenomination.to(TimeUnit.NANOSECONDS);
    Preconditions.assertTrue(watchTimeoutNanos.getDuration() % watchTimeoutDenominationNanos.getDuration() == 0L,
        () -> "watchTimeout (=" + watchTimeout + ") is not a multiple of watchTimeoutDenomination (="
            + watchTimeoutDenomination + ").");

    final int elementLimit = RaftServerConfigKeys.Watch.elementLimit(properties);
    Arrays.stream(ReplicationLevel.values()).forEach(r -> queues.put(r, new WatchQueue(r, elementLimit)));
  }

  CompletableFuture<Void> add(RaftClientRequest request) {
    final WatchRequestTypeProto watch = request.getType().getWatch();
    final WatchQueue queue = queues.get(watch.getReplication());
    if (watch.getIndex() > queue.getIndex()) { // compare without synchronization
      final CompletableFuture<Void> future = queue.add(request);
      if (future != null) {
        return future;
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  void update(ReplicationLevel replication, final long newIndex) {
    final WatchQueue queue = queues.get(replication);
    if (newIndex > queue.getIndex()) { // compare without synchronization
      queue.updateIndex(newIndex);
    }
  }

  void failWatches(Exception e) {
    queues.values().forEach(q -> q.failAll(e));
  }
}
