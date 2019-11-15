/**
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
package org.apache.ratis.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.RetryPolicy;
import java.time.Duration;
import java.io.IOException;

/*
 * An interface to hold various I/O retry policies
 */
public interface IORetryPolicy {
  Logger LOG = LoggerFactory.getLogger(IORetryPolicy.class);

  public final RetryPolicy<Object> retryPolicy = new RetryPolicy<Object>()
      .handle(IOException.class)
      .onRetry(e -> LOG.warn("Retrying:", (e.getLastFailure() != null ? e.getLastFailure() :e.getLastResult())))
      .onFailure(e -> LOG.error("Failed:", e.getFailure()))
      .withDelay(Duration.ofNanos(1))
      .withMaxAttempts(-1);

  public final RetryPolicy<Boolean> booleanCheckingRetryPolicy = new RetryPolicy<Boolean>()
      .handle(IOException.class)
      .handleResult(false)
      .onRetry(e -> LOG.warn("Retrying boolean:", e.getLastFailure()))
      .onFailure(e -> LOG.error("Failed to get true for:", e.getFailure()))
      .withDelay(Duration.ofNanos(1))
      .withMaxAttempts(-1);
} 
