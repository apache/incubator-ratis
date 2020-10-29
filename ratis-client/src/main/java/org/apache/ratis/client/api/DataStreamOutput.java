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
package org.apache.ratis.client.api;

import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.protocol.DataStreamReply;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** An asynchronous output stream supporting zero buffer copying. */
public interface DataStreamOutput extends CloseAsync<DataStreamReply> {
  /** Send out the data in the buffer asynchronously */
  CompletableFuture<DataStreamReply> writeAsync(ByteBuffer buf);

  /** Create a transaction asynchronously once the stream data is replicated to all servers */
  CompletableFuture<DataStreamReply> startTransactionAsync();

  /** Get the future of the header request. */
  CompletableFuture<DataStreamReply> getHeaderFuture();
}
