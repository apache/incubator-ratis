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
package org.apache.ratis.datastream;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.util.SizeInBytes;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;


/**
 * An interface for streaming data.
 * Associated with it's implementation will be a client.
 */

public interface DataStreamApi {

  /**
   * Create a new stream for a new streamToRatis invocation
   * allows multiple stream from a single client.
   */
  OutboundDataStream newStream();

  /**
   * stream large files to raft group from client.
   * Returns a future of the final stream packet to indicate completion of stream.
   * Bytebuffer needs to be direct for zero-copy semantics.
   *
   */
  CompletableFuture<DataStreamReply> streamToRatis(ByteBuffer message, SizeInBytes packetSize);

  /**
   *  Same as streamToRatis with default packet size.
   */
  CompletableFuture<DataStreamReply> streamToRatis(ByteBuffer message);

}