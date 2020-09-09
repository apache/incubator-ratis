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

package org.apache.ratis.grpc.metrics.intercept.client;

import org.apache.ratis.grpc.metrics.MessageMetrics;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;

/**
 * An implementation of a client interceptor.
 * Intercepts the messages and increments metrics accordingly
 * before sending them.
 */

public class MetricClientInterceptor implements ClientInterceptor {
  private final String identifier;
  private final MessageMetrics metrics;

  public MetricClientInterceptor(String identifier){
    this.identifier = identifier;
    this.metrics = new MessageMetrics(identifier, "client");
  }

  private String getMethodMetricPrefix(MethodDescriptor<?, ?> method){
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return identifier + "_" + serviceName + "_" + methodName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                             CallOptions callOptions,
                                                             Channel channel) {

    return new MetricClientCall<>(
        channel.newCall(methodDescriptor, callOptions),
        metrics,
        getMethodMetricPrefix(methodDescriptor)
    );
  }
}
