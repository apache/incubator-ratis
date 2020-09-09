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
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.client.GrpcClientProtocolService;
import org.apache.ratis.grpc.metrics.intercept.server.MetricServerInterceptor;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;

import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.OPENSSL;

/** A grpc implementation of {@link RaftServerRpc}. */
public final class GrpcService extends RaftServerRpcWithProxy<GrpcServerProtocolClient,
    PeerProxyMap<GrpcServerProtocolClient>> {
  static final Logger LOG = LoggerFactory.getLogger(GrpcService.class);
  public static final String GRPC_SEND_SERVER_REQUEST =
      GrpcService.class.getSimpleName() + ".sendRequest";

  public static final class Builder extends RaftServerRpc.Builder<Builder, GrpcService> {
    private GrpcTlsConfig tlsConfig;

    private Builder() {}

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public GrpcService build() {
      return new GrpcService(getServer(), getTlsConfig());
    }

    public Builder setTlsConfig(GrpcTlsConfig tlsConfig) {
      this.tlsConfig = tlsConfig;
      return this;
    }

    public GrpcTlsConfig getTlsConfig() {
      return tlsConfig;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Server server;
  private final Supplier<InetSocketAddress> addressSupplier;

  private final GrpcClientProtocolService clientProtocolService;

  private final MetricServerInterceptor serverInterceptor;

  public MetricServerInterceptor getServerInterceptor() {
    return serverInterceptor;
  }

  private GrpcService(RaftServer server, GrpcTlsConfig tlsConfig) {
    this(server, server::getId,
        GrpcConfigKeys.Server.port(server.getProperties()),
        GrpcConfigKeys.messageSizeMax(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Log.Appender.bufferByteLimit(server.getProperties()),
        GrpcConfigKeys.flowControlWindow(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Rpc.requestTimeout(server.getProperties()),
        tlsConfig);
  }

  @SuppressWarnings("checkstyle:ParameterNumber") // private constructor
  private GrpcService(RaftServer raftServer, Supplier<RaftPeerId> idSupplier, int port,
      SizeInBytes grpcMessageSizeMax, SizeInBytes appenderBufferSize,
      SizeInBytes flowControlWindow, TimeDuration requestTimeoutDuration, GrpcTlsConfig tlsConfig) {
    super(idSupplier, id -> new PeerProxyMap<>(id.toString(),
        p -> new GrpcServerProtocolClient(p, flowControlWindow.getSizeInt(),
            requestTimeoutDuration, tlsConfig)));
    if (appenderBufferSize.getSize() > grpcMessageSizeMax.getSize()) {
      throw new IllegalArgumentException("Illegal configuration: "
          + RaftServerConfigKeys.Log.Appender.BUFFER_BYTE_LIMIT_KEY + " = " + appenderBufferSize
          + " > " + GrpcConfigKeys.MESSAGE_SIZE_MAX_KEY + " = " + grpcMessageSizeMax);
    }

    this.clientProtocolService = new GrpcClientProtocolService(idSupplier, raftServer);

    this.serverInterceptor = new MetricServerInterceptor(
        idSupplier,
        getClass().getSimpleName() + "_" + Integer.toString(port)
    );

    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .withChildOption(ChannelOption.SO_REUSEADDR, true)
        .maxInboundMessageSize(grpcMessageSizeMax.getSizeInt())
        .flowControlWindow(flowControlWindow.getSizeInt())
        .addService(ServerInterceptors.intercept(
            new GrpcServerProtocolService(idSupplier, raftServer),
            serverInterceptor))
        .addService(ServerInterceptors.intercept(clientProtocolService, serverInterceptor))
        .addService(ServerInterceptors.intercept(
            new GrpcAdminProtocolService(raftServer),
            serverInterceptor));

    if (tlsConfig != null) {
      SslContextBuilder sslContextBuilder =
          tlsConfig.isFileBasedConfig()?
              SslContextBuilder.forServer(tlsConfig.getCertChainFile(),
                  tlsConfig.getPrivateKeyFile()):
              SslContextBuilder.forServer(tlsConfig.getPrivateKey(),
                  tlsConfig.getCertChain());
      if (tlsConfig.getMtlsEnabled()) {
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        if (tlsConfig.isFileBasedConfig()) {
          sslContextBuilder.trustManager(tlsConfig.getTrustStoreFile());
        } else {
            sslContextBuilder.trustManager(tlsConfig.getTrustStore());
        }
      }
      sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder, OPENSSL);
      try {
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to build SslContext, tlsConfig=" + tlsConfig, ex);
      }
    }
    server = nettyServerBuilder.build();
    addressSupplier = JavaUtils.memoize(() -> new InetSocketAddress(port != 0? port: server.getPort()));
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public void startImpl() {
    try {
      server.start();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Failed to start Grpc server", e, LOG);
    }
    LOG.info("{}: {} started, listening on {}", getId(), getClass().getSimpleName(), getInetSocketAddress());
  }

  @Override
  public void closeImpl() throws IOException {
    final String name = getId() + ": shutdown server with port " + server.getPort();
    LOG.info("{} now", name);
    final Server s = server.shutdownNow();
    super.closeImpl();
    try {
      s.awaitTermination();
    } catch(InterruptedException e) {
      throw IOUtils.toInterruptedIOException(name + " failed", e);
    }
    LOG.info("{} successfully", name);
  }

  @Override
  public void notifyNotLeader(RaftGroupId groupId) {
    clientProtocolService.closeAllOrderedRequestStreamObservers(groupId);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return addressSupplier.get();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(),
        null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).requestVote(request);
  }
}
