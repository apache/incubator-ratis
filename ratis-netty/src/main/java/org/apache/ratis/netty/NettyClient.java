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

import java.io.Closeable;
import java.net.InetSocketAddress;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;

public class NettyClient implements Closeable {
  private final LifeCycle lifeCycle = new LifeCycle(JavaUtils.getClassSimpleName(getClass()));

  private Channel channel;

  /** Connects to the given server address. */
  public void connect(String serverAddress, EventLoopGroup group,
                      ChannelInitializer<SocketChannel> initializer)
      throws InterruptedException {
    final InetSocketAddress address = NetUtils.createSocketAddr(serverAddress);

    final Class<? extends SocketChannel> socketChannel;
    if (group instanceof EpollEventLoopGroup) {
      socketChannel = EpollSocketChannel.class;
    } else {
      socketChannel = NioSocketChannel.class;
    }

    lifeCycle.startAndTransition(
        () -> channel = new Bootstrap()
            .group(group)
            .channel(socketChannel)
            .handler(new LoggingHandler(LogLevel.INFO))
            .handler(initializer)
            .connect(address)
            .sync()
            .channel(),
        InterruptedException.class);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      channel.close().syncUninterruptibly();
    });
  }

  public ChannelFuture writeAndFlush(Object msg) {
    lifeCycle.assertCurrentState(LifeCycle.States.RUNNING);
    return channel.writeAndFlush(msg);
  }
}
