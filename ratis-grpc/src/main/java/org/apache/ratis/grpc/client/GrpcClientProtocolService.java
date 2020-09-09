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
package org.apache.ratis.grpc.client;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceImplBase;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GrpcClientProtocolService extends RaftClientProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolService.class);

  private static class PendingOrderedRequest implements SlidingWindow.ServerSideRequest<RaftClientReply> {
    private final RaftClientRequest request;
    private final AtomicReference<RaftClientReply> reply = new AtomicReference<>();

    PendingOrderedRequest(RaftClientRequest request) {
      this.request = request;
    }

    @Override
    public void fail(Throwable t) {
      Preconditions.assertTrue(t instanceof RaftException, () -> "Requires RaftException but " + t);
      setReply(new RaftClientReply(request, (RaftException) t, null));
    }

    @Override
    public boolean hasReply() {
      return getReply() != null || this == COMPLETED;
    }

    @Override
    public void setReply(RaftClientReply r) {
      final boolean set = reply.compareAndSet(null, r);
      Preconditions.assertTrue(set, () -> "Reply is already set: request=" + request + ", reply=" + reply);
    }

    RaftClientReply getReply() {
      return reply.get();
    }

    RaftClientRequest getRequest() {
      return request;
    }

    @Override
    public long getSeqNum() {
      return request != null? request.getSlidingWindowEntry().getSeqNum(): Long.MAX_VALUE;
    }

    @Override
    public boolean isFirstRequest() {
      return request != null && request.getSlidingWindowEntry().getIsFirst();
    }

    @Override
    public String toString() {
      return request != null? getSeqNum() + ":" + reply: "COMPLETED";
    }
  }
  private static final PendingOrderedRequest COMPLETED = new PendingOrderedRequest(null);

  static class OrderedStreamObservers {
    private final Map<Integer, OrderedRequestStreamObserver> map = new ConcurrentHashMap<>();

    void putNew(OrderedRequestStreamObserver so) {
      CollectionUtils.putNew(so.getId(), so, map, () -> getClass().getSimpleName());
    }

    void removeExisting(OrderedRequestStreamObserver so) {
      CollectionUtils.removeExisting(so.getId(), so, map, () -> getClass().getSimpleName());
    }

    void closeAllExisting(RaftGroupId groupId) {
      // Iteration not synchronized:
      // Okay if an existing object is removed by another mean during the iteration since it must be already closed.
      // Also okay if a new object is added during the iteration since this method closes only the existing objects.
      for(Iterator<Map.Entry<Integer, OrderedRequestStreamObserver>> i = map.entrySet().iterator(); i.hasNext(); ) {
        final OrderedRequestStreamObserver so = i.next().getValue();
        final RaftGroupId gid = so.getGroupId();
        if (gid == null || gid.equals(groupId)) {
          so.close(true);
          i.remove();
        }
      }
    }
  }

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftClientAsynchronousProtocol protocol;

  private final OrderedStreamObservers orderedStreamObservers = new OrderedStreamObservers();

  public GrpcClientProtocolService(Supplier<RaftPeerId> idSupplier, RaftClientAsynchronousProtocol protocol) {
    this.idSupplier = idSupplier;
    this.protocol = protocol;
  }

  RaftPeerId getId() {
    return idSupplier.get();
  }

  @Override
  public void setConfiguration(SetConfigurationRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final SetConfigurationRequest request = ClientProtoUtils.toSetConfigurationRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.setConfigurationAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public StreamObserver<RaftClientRequestProto> ordered(StreamObserver<RaftClientReplyProto> responseObserver) {
    final OrderedRequestStreamObserver so = new OrderedRequestStreamObserver(responseObserver);
    orderedStreamObservers.putNew(so);
    return so;
  }

  public void closeAllOrderedRequestStreamObservers(RaftGroupId groupId) {
    LOG.debug("{}: closeAllOrderedRequestStreamObservers", getId());
    orderedStreamObservers.closeAllExisting(groupId);
  }

  @Override
  public StreamObserver<RaftClientRequestProto> unordered(StreamObserver<RaftClientReplyProto> responseObserver) {
    return new UnorderedRequestStreamObserver(responseObserver);
  }

  private final AtomicInteger streamCount = new AtomicInteger();

  private abstract class RequestStreamObserver implements StreamObserver<RaftClientRequestProto> {
    private final int id = streamCount.getAndIncrement();
    private final String name = getId() + "-" + getClass().getSimpleName() + id;
    private final StreamObserver<RaftClientReplyProto> responseObserver;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    RequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      LOG.debug("new {}", name);
      this.responseObserver = responseObserver;
    }

    int getId() {
      return id;
    }

    String getName() {
      return name;
    }

    synchronized void responseNext(RaftClientReplyProto reply) {
      responseObserver.onNext(reply);
    }

    synchronized void responseCompleted() {
      try {
        responseObserver.onCompleted();
      } catch(Exception e) {
        // response stream may possibly be already closed/failed so that the exception can be safely ignored.
        if (LOG.isTraceEnabled()) {
          LOG.trace(getName() + ": Failed onCompleted, exception is ignored", e);
        }
      }
    }

    synchronized void responseError(Throwable t) {
      try {
        responseObserver.onError(t);
      } catch(Exception e) {
        // response stream may possibly be already closed/failed so that the exception can be safely ignored.
        if (LOG.isTraceEnabled()) {
          LOG.trace(getName() + ": Failed onError, exception is ignored", e);
        }
      }
    }


    boolean setClose() {
      return isClosed.compareAndSet(false, true);
    }

    boolean isClosed() {
      return isClosed.get();
    }

    CompletableFuture<Void> processClientRequest(RaftClientRequest request, Consumer<RaftClientReply> replyHandler) {
      try {
        String errMsg = LOG.isDebugEnabled() ? "processClientRequest for " + request.toString() : "";
        return protocol.submitClientRequestAsync(request
        ).thenAcceptAsync(replyHandler
        ).exceptionally(exception -> {
          // TODO: the exception may be from either raft or state machine.
          // Currently we skip all the following responses when getting an
          // exception from the state machine.
          responseError(exception, () -> errMsg);
          return null;
        });
      } catch (IOException e) {
        throw new CompletionException("Failed processClientRequest for " + request + " in " + name, e);
      }
    }

    abstract void processClientRequest(RaftClientRequest request);

    @Override
    public void onNext(RaftClientRequestProto request) {
      try {
        final RaftClientRequest r = ClientProtoUtils.toRaftClientRequest(request);
        processClientRequest(r);
      } catch (Throwable e) {
        responseError(e, () -> "onNext for " + ClientProtoUtils.toString(request) + " in " + name);
      }
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      GrpcUtil.warn(LOG, () -> name + ": onError", t);
    }


    boolean responseError(Throwable t, Supplier<String> message) {
      if (setClose()) {
        t = JavaUtils.unwrapCompletionException(t);
        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": Failed " + message.get(), t);
        }
        responseError(GrpcUtil.wrapException(t));
        return true;
      }
      return false;
    }
  }

  private class UnorderedRequestStreamObserver extends RequestStreamObserver {
    /** Map: callId -> futures (seqNum is not set for unordered requests) */
    private final Map<Long, CompletableFuture<Void>> futures = new HashMap<>();

    UnorderedRequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      super(responseObserver);
    }

    @Override
    void processClientRequest(RaftClientRequest request) {
      final CompletableFuture<Void> f = processClientRequest(request, reply -> {
        if (!reply.isSuccess()) {
          LOG.info("Failed " + request + ", reply=" + reply);
        }
        final RaftClientReplyProto proto = ClientProtoUtils.toRaftClientReplyProto(reply);
        responseNext(proto);
      });
      final long callId = request.getCallId();
      put(callId, f);
      f.thenAccept(dummy -> remove(callId));
    }

    private synchronized void put(long callId, CompletableFuture<Void> f) {
      futures.put(callId, f);
    }
    private synchronized void remove(long callId) {
      futures.remove(callId);
    }

    private synchronized CompletableFuture<Void> allOfFutures() {
      return JavaUtils.allOf(futures.values());
    }

    @Override
    public void onCompleted() {
      allOfFutures().thenAccept(dummy -> {
        if (setClose()) {
          LOG.debug("{}: close", getName());
          responseCompleted();
        }
      });
    }
  }

  private class OrderedRequestStreamObserver extends RequestStreamObserver {
    private final SlidingWindow.Server<PendingOrderedRequest, RaftClientReply> slidingWindow
        = new SlidingWindow.Server<>(getName(), COMPLETED);
    /** The {@link RaftGroupId} for this observer. */
    private final AtomicReference<RaftGroupId> groupId = new AtomicReference<>();

    OrderedRequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      super(responseObserver);
    }

    RaftGroupId getGroupId() {
      return groupId.get();
    }

    void processClientRequest(PendingOrderedRequest pending) {
      final long seq = pending.getSeqNum();
      processClientRequest(pending.getRequest(),
          reply -> slidingWindow.receiveReply(seq, reply, this::sendReply));
    }

    @Override
    void processClientRequest(RaftClientRequest r) {
      if (isClosed()) {
        final AlreadyClosedException exception = new AlreadyClosedException(getName() + ": the stream is closed");
        responseError(exception, () -> "processClientRequest (stream already closed) for " + r);
      }

      final RaftGroupId requestGroupId = r.getRaftGroupId();
      // use the group id in the first request as the group id of this observer
      final RaftGroupId updated = groupId.updateAndGet(g -> g != null ? g: requestGroupId);
      final PendingOrderedRequest pending = new PendingOrderedRequest(r);

      if (!requestGroupId.equals(updated)) {
        final GroupMismatchException exception = new GroupMismatchException(getId()
            + ": The group (" + requestGroupId + ") of " + r.getClientId()
            + " does not match the group (" + updated + ") of the " + getClass().getSimpleName());
        responseError(exception, () -> "processClientRequest (Group mismatched) for " + r);
        return;
      }

      slidingWindow.receivedRequest(pending, this::processClientRequest);
    }

    private void sendReply(PendingOrderedRequest ready) {
      Preconditions.assertTrue(ready.hasReply());
      if (ready == COMPLETED) {
        close(true);
      } else {
        LOG.debug("{}: sendReply seq={}, {}", getName(), ready.getSeqNum(), ready.getReply());
        responseNext(ClientProtoUtils.toRaftClientReplyProto(ready.getReply()));
      }
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      GrpcUtil.warn(LOG, () -> getName() + ": onError", t);
      close(false);
    }

    @Override
    public void onCompleted() {
      if (slidingWindow.endOfRequests(this::sendReply)) {
        close(true);
      }
    }

    private void close(boolean complete) {
      if (setClose()) {
        LOG.debug("{}: close", getName());
        if (complete) {
          responseCompleted();
        }
        cleanup();
      }
    }

    private void cleanup() {
      slidingWindow.close();
      orderedStreamObservers.removeExisting(this);
    }

    @Override
    boolean responseError(Throwable t, Supplier<String> message) {
      if (super.responseError(t, message)) {
        cleanup();
        return true;
      }
      return false;
    }
  }
}
