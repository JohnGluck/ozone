/*

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for communication with a datanode.
 * Provides methods to perform any protocol calls by Container clients on a single datanode.
 */
public interface ContainerApi extends AutoCloseable {
  ListBlockResponseProto listBlock(long containerId, Long startLocalId, int count) throws IOException;

  GetBlockResponseProto getBlock(BlockID blockId, Map<DatanodeDetails, Integer> replicaIndexes) throws IOException;

  GetCommittedBlockLengthResponseProto getCommittedBlockLength(BlockID blockId) throws IOException;

  @Override
  void close();
}
