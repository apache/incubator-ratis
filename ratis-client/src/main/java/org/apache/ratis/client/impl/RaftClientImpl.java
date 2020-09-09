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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.api.StreamApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** A client who sends requests to a raft service. */
public final class RaftClientImpl implements RaftClient {
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  public abstract static class PendingClientRequest {
    private final long creationTimeInMs = System.currentTimeMillis();
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
    private final AtomicInteger attemptCount = new AtomicInteger();
    private final Map<Class<?>, Integer> exceptionCount = new ConcurrentHashMap<>();

    public abstract RaftClientRequest newRequestImpl();

    final RaftClientRequest newRequest() {
      attemptCount.incrementAndGet();
      return newRequestImpl();
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    public int getAttemptCount() {
      return attemptCount.get();
    }

    int incrementExceptionCount(Throwable t) {
      return t != null ? exceptionCount.compute(t.getClass(), (k, v) -> v != null ? v + 1 : 1) : 0;
    }

    public int getExceptionCount(Throwable t) {
      return t != null ? Optional.ofNullable(exceptionCount.get(t.getClass())).orElse(0) : 0;
    }

    public boolean isRequestTimeout(TimeDuration timeout) {
      if (timeout == null) {
        return false;
      }
      return System.currentTimeMillis() - creationTimeInMs > timeout.toLong(TimeUnit.MILLISECONDS);
    }
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final Collection<RaftPeer> peers;
  private final RaftGroupId groupId;
  private final RetryPolicy retryPolicy;

  private volatile RaftPeerId leaderId;

  private final TimeoutScheduler scheduler;

  private final Supplier<OrderedAsync> orderedAsync;
  private final Supplier<StreamApi> streamApi;

  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId,
      RaftClientRpc clientRpc, RaftProperties properties, RetryPolicy retryPolicy) {
    this.clientId = clientId;
    this.clientRpc = clientRpc;
    this.peers = new ConcurrentLinkedQueue<>(group.getPeers());
    this.groupId = group.getGroupId();
    this.leaderId = leaderId != null? leaderId : getHighestPriorityPeerId();
    Preconditions.assertTrue(retryPolicy != null, "retry policy can't be null");
    this.retryPolicy = retryPolicy;

    scheduler = TimeoutScheduler.getInstance();
    clientRpc.addServers(peers);

    this.orderedAsync = JavaUtils.memoize(() -> OrderedAsync.newInstance(this, properties));
    this.streamApi = JavaUtils.memoize(() -> StreamImpl.newInstance(this, properties));
  }

  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  private RaftPeerId getHighestPriorityPeerId() {
    if (peers == null) {
      return null;
    }

    int maxPriority = Integer.MIN_VALUE;
    RaftPeerId highestPriorityPeerId = null;
    for (RaftPeer peer : peers) {
      if (maxPriority < peer.getPriority()) {
        maxPriority = peer.getPriority();
        highestPriorityPeerId = peer.getId();
      }
    }

    return highestPriorityPeerId;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  TimeDuration getEffectiveSleepTime(Throwable t, TimeDuration sleepDefault) {
    return t instanceof NotLeaderException && ((NotLeaderException) t).getSuggestedLeader() != null ?
        TimeDuration.ZERO : sleepDefault;
  }

  TimeoutScheduler getScheduler() {
    return scheduler;
  }

  OrderedAsync getOrderedAsync() {
    return orderedAsync.get();
  }

  @Override
  public StreamApi getStreamApi() {
    return streamApi.get();
  }

  @Override
  public CompletableFuture<RaftClientReply> sendAsync(Message message) {
    return sendAsync(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnlyAsync(Message message) {
    return sendAsync(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendStaleReadAsync(Message message, long minIndex, RaftPeerId server) {
    return sendAsync(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendWatchAsync(long index, ReplicationLevel replication) {
    return UnorderedAsync.send(RaftClientRequest.watchRequestType(index, replication), this);
  }

  CompletableFuture<RaftClientReply> streamAsync(long streamId, long messageId, Message message, boolean endOfRequest) {
    return sendAsync(RaftClientRequest.streamRequestType(streamId, messageId, endOfRequest), message, null);
  }

  CompletableFuture<RaftClientReply> streamCloseAsync(long streamId, long messageId) {
    return sendAsync(RaftClientRequest.streamRequestType(streamId, messageId, true), null, null);
  }

  private CompletableFuture<RaftClientReply> sendAsync(
      RaftClientRequest.Type type, Message message, RaftPeerId server) {
    return getOrderedAsync().send(type, message, server);
  }

  RaftClientRequest newRaftClientRequest(
      RaftPeerId server, long callId, Message message, RaftClientRequest.Type type,
      SlidingWindowEntry slidingWindowEntry) {
    return new RaftClientRequest(clientId, server != null? server: leaderId, groupId,
        callId, message, type, slidingWindowEntry);
  }

  @Override
  public RaftClientReply send(Message message) throws IOException {
    return send(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendReadOnly(Message message) throws IOException {
    return send(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server)
      throws IOException {
    return send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public RaftClientReply sendWatch(long index, ReplicationLevel replication) throws IOException {
    return send(RaftClientRequest.watchRequestType(index, replication), null, null);
  }

  private RaftClientReply send(RaftClientRequest.Type type, Message message, RaftPeerId server)
      throws IOException {
    if (!type.is(TypeCase.WATCH)) {
      Objects.requireNonNull(message, "message == null");
    }

    final long callId = nextCallId();
    return sendRequestWithRetry(() -> newRaftClientRequest(server, callId, message, type, null));
  }

  // TODO: change peersInNewConf to List<RaftPeer>
  @Override
  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    Objects.requireNonNull(peersInNewConf, "peersInNewConf == null");

    final long callId = nextCallId();
    // also refresh the rpc proxies for these peers
    addServers(Arrays.stream(peersInNewConf));
    return sendRequestWithRetry(() -> new SetConfigurationRequest(
        clientId, leaderId, groupId, callId, Arrays.asList(peersInNewConf)));
  }

  @Override
  public RaftClientReply groupAdd(RaftGroup newGroup, RaftPeerId server) throws IOException {
    Objects.requireNonNull(newGroup, "newGroup == null");
    Objects.requireNonNull(server, "server == null");

    final long callId = nextCallId();
    addServers(newGroup.getPeers().stream());
    return sendRequest(GroupManagementRequest.newAdd(clientId, server, callId, newGroup));
  }

  @Override
  public RaftClientReply groupRemove(RaftGroupId grpId, boolean deleteDirectory,
      boolean renameDirectory, RaftPeerId server)
      throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");
    Objects.requireNonNull(server, "server == null");

    final long callId = nextCallId();
    return sendRequest(GroupManagementRequest.newRemove(clientId, server,
        callId, grpId, deleteDirectory, renameDirectory));
  }

  @Override
  public GroupListReply getGroupList(RaftPeerId server) throws IOException {
    Objects.requireNonNull(server, "server == null");

    final RaftClientReply reply = sendRequest(new GroupListRequest(clientId, server, groupId, nextCallId()));
    Preconditions.assertTrue(reply instanceof GroupListReply, () -> "Unexpected reply: " + reply);
    return (GroupListReply)reply;
  }

  @Override
  public GroupInfoReply getGroupInfo(RaftGroupId raftGroupId, RaftPeerId server) throws IOException {
    Objects.requireNonNull(server, "server == null");
    RaftGroupId rgi = raftGroupId == null ? groupId : raftGroupId;
    final RaftClientReply reply = sendRequest(new GroupInfoRequest(clientId, server, rgi, nextCallId()));
    Preconditions.assertTrue(reply instanceof GroupInfoReply, () -> "Unexpected reply: " + reply);
    return (GroupInfoReply)reply;
  }

  private void addServers(Stream<RaftPeer> peersInNewConf) {
    clientRpc.addServers(
        peersInNewConf.filter(p -> !peers.contains(p))::iterator);
  }

  private RaftClientReply sendRequestWithRetry(Supplier<RaftClientRequest> supplier) throws IOException {
    PendingClientRequest pending = new PendingClientRequest() {
      @Override
      public RaftClientRequest newRequestImpl() {
        return supplier.get();
      }
    };
    while (true) {
      final RaftClientRequest request = pending.newRequest();
      IOException ioe = null;
      try {
        final RaftClientReply reply = sendRequest(request);

        if (reply != null) {
          return reply;
        }
      } catch (GroupMismatchException | StateMachineException e) {
        throw e;
      } catch (IOException e) {
        ioe = e;
      }

      pending.incrementExceptionCount(ioe);
      ClientRetryEvent event = new ClientRetryEvent(request, ioe, pending);
      final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);
      TimeDuration sleepTime = getEffectiveSleepTime(ioe, action.getSleepTime());

      if (!action.shouldRetry()) {
        throw (IOException)noMoreRetries(event);
      }

      try {
        sleepTime.sleep();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("retry policy=" + retryPolicy);
      }
    }
  }

  Throwable noMoreRetries(ClientRetryEvent event) {
    final int attemptCount = event.getAttemptCount();
    final Throwable throwable = event.getCause();
    if (attemptCount == 1 && throwable != null) {
      return throwable;
    }
    return new RaftRetryFailureException(event.getRequest(), attemptCount, retryPolicy, throwable);
  }

  private RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    LOG.debug("{}: send {}", clientId, request);
    RaftClientReply reply;
    try {
      reply = clientRpc.sendRequest(request);
    } catch (GroupMismatchException gme) {
      throw gme;
    } catch (IOException ioe) {
      handleIOException(request, ioe);
      throw ioe;
    }
    LOG.debug("{}: receive {}", clientId, reply);
    reply = handleLeaderException(request, reply, null);
    reply = handleRaftException(reply, Function.identity());
    return reply;
  }

  static <E extends Throwable> RaftClientReply handleRaftException(
      RaftClientReply reply, Function<RaftException, E> converter) throws E {
    if (reply != null) {
      final RaftException e = reply.getException();
      if (e != null) {
        throw converter.apply(e);
      }
    }
    return reply;
  }

  /**
   * @return null if the reply is null or it has
   * {@link NotLeaderException} or {@link LeaderNotReadyException}
   * otherwise return the same reply.
   */
  RaftClientReply handleLeaderException(RaftClientRequest request, RaftClientReply reply,
                                        Consumer<RaftClientRequest> handler) {
    if (reply == null || reply.getException() instanceof LeaderNotReadyException) {
      return null;
    }
    final NotLeaderException nle = reply.getNotLeaderException();
    if (nle == null) {
      return reply;
    }
    return handleNotLeaderException(request, nle, handler);
  }

  RaftClientReply handleNotLeaderException(RaftClientRequest request, NotLeaderException nle,
      Consumer<RaftClientRequest> handler) {
    refreshPeers(nle.getPeers());
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader, handler);
    return null;
  }

  private void refreshPeers(Collection<RaftPeer> newPeers) {
    if (newPeers != null && newPeers.size() > 0) {
      peers.clear();
      peers.addAll(newPeers);
      // also refresh the rpc proxies for these peers
      clientRpc.addServers(newPeers);
    }
  }

  void handleIOException(RaftClientRequest request, IOException ioe) {
    handleIOException(request, ioe, null, null);
  }

  void handleIOException(RaftClientRequest request, IOException ioe,
      RaftPeerId newLeader, Consumer<RaftClientRequest> handler) {
    LOG.debug("{}: suggested new leader: {}. Failed {} with {}",
        clientId, newLeader, request, ioe);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Stack trace", new Throwable("TRACE"));
    }

    Optional.ofNullable(handler).ifPresent(h -> h.accept(request));

    if (ioe instanceof LeaderNotReadyException || ioe instanceof ResourceUnavailableException) {
      return;
    }

    final RaftPeerId oldLeader = request.getServerId();
    final RaftPeerId curLeader = leaderId;
    final boolean stillLeader = oldLeader.equals(curLeader);
    if (newLeader == null && stillLeader) {
      newLeader = CollectionUtils.random(oldLeader,
          CollectionUtils.as(peers, RaftPeer::getId));
    }
    LOG.debug("{}: oldLeader={},  curLeader={}, newLeader={}", clientId, oldLeader, curLeader, newLeader);

    final boolean changeLeader = newLeader != null && stillLeader;
    final boolean reconnect = changeLeader || clientRpc.shouldReconnect(ioe);
    if (reconnect) {
      if (changeLeader && oldLeader.equals(leaderId)) {
        LOG.debug("{} {}: client change Leader from {} to {} ex={}", groupId,
            clientId, oldLeader, newLeader, ioe.getClass().getName());
        this.leaderId = newLeader;
      }
      clientRpc.handleException(oldLeader, ioe, true);
    }
  }

  long getCallId() {
    return CALL_ID_COUNTER.get();
  }

  @Override
  public RaftClientRpc getClientRpc() {
    return clientRpc;
  }

  @Override
  public void close() throws IOException {
    scheduler.close();
    clientRpc.close();
  }
}
