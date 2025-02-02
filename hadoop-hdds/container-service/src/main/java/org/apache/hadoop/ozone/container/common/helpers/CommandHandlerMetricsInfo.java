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

import org.apache.hadoop.metrics2.MetricsInfo;

/** Different command handler metrics. */
enum CommandHandlerMetricsInfo implements MetricsInfo {
  Command("The type of the SCM command"),
  TotalRunTimeMs("The total runtime of the command handler in milliseconds"),
  AvgRunTimeMs("Average run time of the command handler in milliseconds"),
  QueueWaitingTaskCount("The number of queued tasks waiting for execution"),
  InvocationCount("The number of times the command handler has been invoked"),
  ThreadPoolActivePoolSize("The number of active threads in the thread pool"),
  ThreadPoolMaxPoolSize("The maximum number of threads in the thread pool"),
  CommandReceivedCount("The number of received SCM commands for each command type");

  private final String desc;
  CommandHandlerMetricsInfo(String desc) {
    this.desc = desc;
  }

  @Override
  public String description() {
    return desc;
  }
}
