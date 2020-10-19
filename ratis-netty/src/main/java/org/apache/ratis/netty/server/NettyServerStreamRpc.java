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

package org.apache.ratis.netty.server;

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.List;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class NettyServerStreamRpc implements DataStreamServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyServerStreamRpc.class);

  private final RaftPeer raftServer;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final ChannelFuture channelFuture;

  private final StateMachine stateMachine;
  private final ConcurrentMap<Long, CompletableFuture<DataStream>> streams = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, List<DataStreamOutput>> peersStreamOutput = new ConcurrentHashMap<>();

  private List<DataStreamClient> clients = new ArrayList<>();

  public NettyServerStreamRpc(RaftPeer server, StateMachine stateMachine) {
    this.raftServer = server;
    this.stateMachine = stateMachine;
    this.channelFuture = buildChannel();
  }

  public NettyServerStreamRpc(
      RaftPeer server, List<RaftPeer> otherPeers,
      StateMachine stateMachine, RaftProperties properties){
    this(server, stateMachine);
    setupClient(otherPeers, properties);
  }

  private List<DataStreamOutput> getDataStreamOutput() {
    return clients.stream().map(client -> client.stream()).collect(Collectors.toList());
  }

  private CompletableFuture<DataStream> getDataStreamFuture(ByteBuf buf, AtomicBoolean shouldRelease) {
    try {
      final RaftClientRequest request =
          ClientProtoUtils.toRaftClientRequest(RaftProtos.RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      return stateMachine.data().stream(request);
    } catch (InvalidProtocolBufferException e) {
      throw new CompletionException(e);
    } finally {
      shouldRelease.set(true);
    }
  }

  private long writeTo(ByteBuf buf, DataStream stream, boolean shouldRelease) {
    if (shouldRelease) {
      return 0;
    }

      if (stream == null) {
        return 0;
      }

      final WritableByteChannel channel = stream.getWritableByteChannel();
      long byteWritten = 0;
      for (ByteBuffer buffer : buf.nioBuffers()) {
        try {
          byteWritten += channel.write(buffer);
        } catch (Throwable t) {
          throw new CompletionException(t);
        }
      }
      return byteWritten;
  }

  private void sendReply(DataStreamRequestByteBuf request, ChannelHandlerContext ctx, ByteBuf buf) {
    // TODO RATIS-1098: include byteWritten and isSuccess in the reply
    try {
      final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
          request.getStreamId(), request.getStreamOffset(), ByteBuffer.wrap("OK".getBytes()));
      ctx.writeAndFlush(reply);
    } finally {
      buf.release();
    }
  }

  private ChannelInboundHandler getServerHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final DataStreamRequestByteBuf request = (DataStreamRequestByteBuf)msg;
        final ByteBuf buf = request.slice();
        final AtomicBoolean shouldRelease = new AtomicBoolean();
        final boolean isHeader = request.getStreamOffset() == -1;

        CompletableFuture<?>[] parallelWrites = new CompletableFuture<?>[clients.size() + 1];

        final CompletableFuture<?> localWrites = isHeader?
                streams.computeIfAbsent(request.getStreamId(), id -> getDataStreamFuture(buf, shouldRelease))
                : streams.get(request.getStreamId()).thenApply(stream -> writeTo(buf, stream, shouldRelease.get()));
        parallelWrites[0] = localWrites;
        peersStreamOutput.putIfAbsent(request.getStreamId(), getDataStreamOutput());

          // do not need to forward header request
        if (isHeader) {
          for (int i = 0; i < peersStreamOutput.get(request.getStreamId()).size(); i++) {
            parallelWrites[i + 1] = peersStreamOutput.get(request.getStreamId()).get(i).getHeaderFuture();
          }
        } else {
          // body
          for (int i = 0; i < clients.size(); i++) {
            parallelWrites[i + 1]  =
              peersStreamOutput.get(request.getStreamId()).get(i).writeAsync(request.slice().nioBuffer());
          }
        }
        CompletableFuture.allOf(parallelWrites).whenComplete((t, r) -> sendReply(request, ctx, buf));
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new DataStreamRequestDecoder());
        p.addLast(new DataStreamReplyEncoder());
        p.addLast(getServerHandler());
      }
    };
  }

  ChannelFuture buildChannel() {
    return new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getInitializer())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .localAddress(NetUtils.createSocketAddr(raftServer.getAddress()))
        .bind();
  }

  private void setupClient(List<RaftPeer> otherPeers, RaftProperties properties) {
    for (RaftPeer peer : otherPeers) {
      clients.add(DataStreamClient.Builder.getClientBuilder()
              .setParameters(null)
              .setRaftServer(peer)
              .setProperties(properties)
              .build());
    }
  }

  private Channel getChannel() {
    return channelFuture.awaitUninterruptibly().channel();
  }

  @Override
  public void startServer() {
    channelFuture.syncUninterruptibly();
  }

  // TODO: RATIS-1099 build connection with other server automatically.
  public void startClientToPeers() {
    for (DataStreamClient client : clients) {
      client.start();
    }
  }

  @Override
  public void closeServer() {
    final ChannelFuture f = getChannel().close();
    f.syncUninterruptibly();
    bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    try {
      bossGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
      workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupt EventLoopGroup terminate", e);
    }

    for (DataStreamClient client : clients) {
      client.close();
    }
  }
}
