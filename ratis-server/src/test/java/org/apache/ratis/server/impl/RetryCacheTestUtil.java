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
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryCacheTestUtil {
  public static RetryCache createRetryCache(){
    return new RetryCacheImpl(TimeDuration.valueOf(60, TimeUnit.SECONDS), null);
  }

  public static void createEntry(RetryCache cache, LogEntryProto logEntry){
    if(logEntry.hasStateMachineLogEntry()) {
      final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
      getOrCreateEntry(cache, invocationId);
    }
  }

  public static boolean isFailed(RetryCache.Entry entry) {
    return ((RetryCacheImpl.CacheEntry)entry).isFailed();
  }

  public static void assertFailure(RetryCache cache, LogEntryProto logEntry, boolean isFailed) {
    if(logEntry.hasStateMachineLogEntry()) {
      final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
      Assert.assertEquals(isFailed, get(cache, invocationId).isFailed());
    }
  }

  public static RetryCache.Entry getOrCreateEntry(RaftServer.Division server, ClientInvocationId invocationId) {
    return getOrCreateEntryImpl(server.getRetryCache(), invocationId);
  }

  public static RetryCache.Entry getOrCreateEntry(RetryCache retryCache, ClientInvocationId invocationId) {
    return getOrCreateEntryImpl(retryCache, invocationId);
  }

  private static RetryCache.Entry getOrCreateEntryImpl(RetryCache cache, ClientInvocationId invocationId) {
    return ((RetryCacheImpl)cache).getOrCreateEntry(invocationId);
  }

  public static RetryCache.Entry get(RaftServer.Division server, ClientId clientId, long callId) {
    return get(server.getRetryCache(), ClientInvocationId.valueOf(clientId, callId));
  }

  private static RetryCacheImpl.CacheEntry get(RetryCache cache, ClientInvocationId invocationId) {
    return ((RetryCacheImpl)cache).getIfPresent(invocationId);
  }

  public static SegmentedRaftLog newSegmentedRaftLog(RaftGroupMemberId memberId, RetryCache retryCache,
      RaftStorage storage, RaftProperties properties) {
    final RaftServerImpl server = mock(RaftServerImpl.class);
    when(server.getRetryCache()).thenReturn((RetryCacheImpl) retryCache);
    when(server.getMemberId()).thenReturn(memberId);
    doCallRealMethod().when(server).notifyTruncatedLogEntry(any(LogEntryProto.class));
    return new SegmentedRaftLog(memberId, server, null,
        server::notifyTruncatedLogEntry, server::submitUpdateCommitEvent,
        storage, () -> -1, properties);
  }
}
