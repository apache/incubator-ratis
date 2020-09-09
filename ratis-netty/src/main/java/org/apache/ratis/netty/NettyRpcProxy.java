/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.netty;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto.RaftNettyServerReplyCase.EXCEPTIONREPLY;

public class NettyRpcProxy implements Closeable {
  public static class PeerMap extends PeerProxyMap<NettyRpcProxy> {
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final RaftProperties properties;

    public PeerMap(String name, RaftProperties properties) {
      super(name);
      this.properties = properties;
    }

    @Override
    public NettyRpcProxy createProxyImpl(RaftPeer peer)
            throws IOException {
      try {
        return new NettyRpcProxy(peer, properties, group);
      } catch (InterruptedException e) {
        throw IOUtils.toInterruptedIOException("Failed connecting to " + peer, e);
      }
    }

    @Override
    public void close() {
      super.close();
      group.shutdownGracefully();
    }
  }

  public static long getCallId(RaftNettyServerReplyProto proto) {
    switch (proto.getRaftNettyServerReplyCase()) {
      case REQUESTVOTEREPLY:
        return proto.getRequestVoteReply().getServerReply().getCallId();
      case APPENDENTRIESREPLY:
        return proto.getAppendEntriesReply().getServerReply().getCallId();
      case INSTALLSNAPSHOTREPLY:
        return proto.getInstallSnapshotReply().getServerReply().getCallId();
      case RAFTCLIENTREPLY:
        return proto.getRaftClientReply().getRpcReply().getCallId();
      case EXCEPTIONREPLY:
        return proto.getExceptionReply().getRpcReply().getCallId();
      case RAFTNETTYSERVERREPLY_NOT_SET:
        throw new IllegalArgumentException("Reply case not set in proto: "
            + proto.getRaftNettyServerReplyCase());
      default:
        throw new UnsupportedOperationException("Reply case not supported: "
            + proto.getRaftNettyServerReplyCase());
    }
  }


  class Connection implements Closeable {
    private final NettyClient client = new NettyClient();
    private final Queue<CompletableFuture<RaftNettyServerReplyProto>> replies
        = new LinkedList<>();

    Connection(EventLoopGroup group) throws InterruptedException {
      final ChannelInboundHandler inboundHandler
          = new SimpleChannelInboundHandler<RaftNettyServerReplyProto>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                                    RaftNettyServerReplyProto proto) {
          final CompletableFuture<RaftNettyServerReplyProto> future = pollReply();
          if (future == null) {
            throw new IllegalStateException("Request #" + getCallId(proto)
                + " not found");
          }
          if (proto.getRaftNettyServerReplyCase() == EXCEPTIONREPLY) {
            final Object ioe = ProtoUtils.toObject(proto.getExceptionReply().getException());
            future.completeExceptionally((IOException)ioe);
          } else {
            future.complete(proto);
          }
        }
      };
      final ChannelInitializer<SocketChannel> initializer
          = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          final ChannelPipeline p = ch.pipeline();

          p.addLast(new ProtobufVarint32FrameDecoder());
          p.addLast(new ProtobufDecoder(RaftNettyServerReplyProto.getDefaultInstance()));
          p.addLast(new ProtobufVarint32LengthFieldPrepender());
          p.addLast(new ProtobufEncoder());

          p.addLast(inboundHandler);
        }
      };

      client.connect(peer.getAddress(), group, initializer);
    }

    synchronized ChannelFuture offer(RaftNettyServerRequestProto request,
        CompletableFuture<RaftNettyServerReplyProto> reply) {
      replies.offer(reply);
      return client.writeAndFlush(request);
    }

    synchronized CompletableFuture<RaftNettyServerReplyProto> pollReply() {
      return replies.poll();
    }

    @Override
    public synchronized void close() {
      client.close();
      if (!replies.isEmpty()) {
        final IOException e = new IOException("Connection to " + peer + " is closed.");
        replies.stream().forEach(f -> f.completeExceptionally(e));
        replies.clear();
      }
    }
  }

  private final RaftPeer peer;
  private final Connection connection;
  private final TimeDuration requestTimeoutDuration;

  public NettyRpcProxy(RaftPeer peer, RaftProperties properties, EventLoopGroup group) throws InterruptedException {
    this.peer = peer;
    this.connection = new Connection(group);
    this.requestTimeoutDuration = RaftClientConfigKeys.Rpc.requestTimeout(properties);
  }

  @Override
  public void close() {
    connection.close();
  }

  public RaftNettyServerReplyProto send(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final CompletableFuture<RaftNettyServerReplyProto> reply = new CompletableFuture<>();
    final ChannelFuture channelFuture = connection.offer(proto, reply);

    try {
      channelFuture.sync();
      return reply.get(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit());
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException(ProtoUtils.toString(request)
          + " sending from " + peer + " is interrupted.", e);
    } catch (ExecutionException e) {
      throw IOUtils.toIOException(e);
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e.getMessage(), e);
    }
  }
}
