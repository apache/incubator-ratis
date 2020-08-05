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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface OutboundDataStream extends AutoCloseable {
  CompletableFuture<DataStreamReply> sendAsync(ByteBuffer buf, long streamId, long packetId);

  CompletableFuture<DataStreamReply> closeAsync();

  default void close() throws Exception {
    try {
      closeAsync().get();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      throw cause instanceof Exception? (Exception)cause: e;
    }
  }
}