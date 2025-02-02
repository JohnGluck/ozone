/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.AvgRunTimeMs;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.Command;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.CommandReceivedCount;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.InvocationCount;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.QueueWaitingTaskCount;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.ThreadPoolActivePoolSize;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.ThreadPoolMaxPoolSize;
import static org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetricsInfo.TotalRunTimeMs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CommandHandler;

/**
 * This class collects and exposes metrics for CommandHandlerMetrics.
 */
@InterfaceAudience.Private
public final class CommandHandlerMetrics implements MetricsSource {
  public static final String SOURCE_NAME =
      CommandHandlerMetrics.class.getSimpleName();
  private final Map<Type, CommandHandler> handlerMap;
  private final Map<Type, AtomicInteger> commandCount;
  private CommandHandlerMetrics(Map<Type, CommandHandler> handlerMap) {
    this.handlerMap = handlerMap;
    this.commandCount = new HashMap<>();
    handlerMap.forEach((k, v) -> this.commandCount.put(k, new AtomicInteger()));
  }

  /**
   * Creates a new instance of CommandHandlerMetrics and
   * registers it to the DefaultMetricsSystem.
   *
   * @param handlerMap the map of command types to their
   *                  corresponding command handlers
   * @return the registered instance of CommandHandlerMetrics
   */
  public static CommandHandlerMetrics create(
      Map<Type, CommandHandler> handlerMap) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "CommandHandlerMetrics Metrics",
        new CommandHandlerMetrics(handlerMap));
  }

  /**
   * Increases the count of received commands for the specified command type.
   *
   * @param type the type of the command for which the count should be increased
   */
  public void increaseCommandCount(Type type) {
    commandCount.get(type).addAndGet(1);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    for (Map.Entry<Type, CommandHandler> entry : handlerMap.entrySet()) {
      CommandHandler commandHandler = entry.getValue();
      MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME)
          .setContext("CommandHandlerMetrics")
          .tag(Command, commandHandler.getCommandType().name());

      builder.addGauge(TotalRunTimeMs, commandHandler.getTotalRunTime());
      builder.addGauge(AvgRunTimeMs, commandHandler.getAverageRunTime());
      builder.addGauge(QueueWaitingTaskCount, commandHandler.getQueuedCount());
      builder.addGauge(InvocationCount, commandHandler.getInvocationCount());
      int activePoolSize = commandHandler.getThreadPoolActivePoolSize();
      if (activePoolSize >= 0) {
        builder.addGauge(ThreadPoolActivePoolSize, activePoolSize);
      }
      int maxPoolSize = commandHandler.getThreadPoolMaxPoolSize();
      if (maxPoolSize >= 0) {
        builder.addGauge(ThreadPoolMaxPoolSize, maxPoolSize);
      }
      builder.addGauge(CommandReceivedCount,
          commandCount.get(commandHandler.getCommandType()).get());
    }
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
