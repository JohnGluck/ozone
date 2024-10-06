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
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ContainerApi;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManager;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ECBlockOutputStreamEntry} tests.
 */
public class TestECBlockOutputStreamEntry {

  @Test
  public void
      testAcquireDifferentClientForECBlocksOnTheSameHostButDifferentPort() throws IOException {
    PipelineID randomId = PipelineID.randomId();
    ReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("RS-3-2-1024k");
    DatanodeDetails node1 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node2 = aNode("127.0.0.1", "localhost", 2002);
    DatanodeDetails node3 = aNode("127.0.0.1", "localhost", 2003);
    DatanodeDetails node4 = aNode("127.0.0.1", "localhost", 2004);
    DatanodeDetails node5 = aNode("127.0.0.1", "localhost", 2005);
    List<DatanodeDetails> nodes =
        Arrays.asList(node1, node2, node3, node4, node5);
    Pipeline anECPipeline = Pipeline.newBuilder()
        .setId(randomId)
        .setReplicationConfig(ecReplicationConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes)
        .build();
    try (ContainerApiManager manager = new ContainerApiManagerImpl()) {
      HashSet<ContainerApi> clients = new HashSet<>();
      final ECBlockOutputStreamEntry.Builder b = new ECBlockOutputStreamEntry.Builder();
      b.setContainerApiManager(manager)
          .setPipeline(anECPipeline);
      try (ECBlockOutputStreamEntry entry = b.build()) {
        for (int i = 0; i < nodes.size(); i++) {
          clients.add(
              manager.acquireClient(
                  entry.createSingleECBlockPipeline(
                      anECPipeline, nodes.get(i), i
                  )));
        }
        assertEquals(5, clients.size());
      }
    }
  }

  @Test
  public void testAcquireDifferentClientForECBlocksOnTheSameHostWithSomeOnSamePortAlso() throws IOException {
    PipelineID randomId = PipelineID.randomId();
    ReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("RS-3-2-1024k");
    DatanodeDetails node1 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node2 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node3 = aNode("127.0.0.1", "localhost", 2003);
    DatanodeDetails node4 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node5 = aNode("127.0.0.1", "localhost", 2005);
    List<DatanodeDetails> nodes =
        Arrays.asList(node1, node2, node3, node4, node5);
    Pipeline anECPipeline = Pipeline.newBuilder()
        .setId(randomId)
        .setReplicationConfig(ecReplicationConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes)
        .build();
    try (ContainerApiManager manager = new ContainerApiManagerImpl()) {
      HashSet<ContainerApi> clients = new HashSet<>();
      final ECBlockOutputStreamEntry.Builder b = new ECBlockOutputStreamEntry.Builder();
      b.setContainerApiManager(manager)
          .setPipeline(anECPipeline);
      try (ECBlockOutputStreamEntry entry = b.build()) {
        for (int i = 0; i < nodes.size(); i++) {
          clients.add(
              manager.acquireClient(
                  entry.createSingleECBlockPipeline(
                      anECPipeline, nodes.get(i), i
                  )));
        }
        assertEquals(3, clients.size());
      }
    }
  }

  private DatanodeDetails aNode(String ip, String hostName, int port) {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setIpAddress(ip)
        .setHostName(hostName)
        .addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, port))
        .build();
  }
}
