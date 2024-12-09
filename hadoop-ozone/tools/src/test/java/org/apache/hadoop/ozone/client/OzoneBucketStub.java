/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;

import static java.util.Collections.emptyMap;

/**
 * In-memory ozone bucket for testing.
 */
public final class OzoneBucketStub extends OzoneBucket {
  private final ClientProtocol proxy;

  private ReplicationConfig replicationConfig;

  public static Builder newBuilder(ClientProtocol proxy) {
    return new Builder(proxy);
  }

  private OzoneBucketStub(Builder builder, ClientProtocol proxy) {
    super(builder);
    this.proxy = proxy;
    this.replicationConfig = super.getReplicationConfig();
  }
  
  @Override
  public OzoneOutputStream createKey(String key, long size) throws IOException {
    return createKey(key, size, getReplicationConfig(), getMetadata(), emptyMap());
  }

  @Override
  public OzoneOutputStream createKey(String key, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return proxy.createKey(
        getVolumeName(),
        getName(),
        key,
        size,
        replicationConfig,
        metadata
    );
  }

  @Override
  public OzoneOutputStream rewriteKey(String keyName, long size, long existingKeyGeneration, ReplicationConfig rConfig,
      Map<String, String> metadata) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String key, long size, ReplicationConfig rConfig,
      Map<String, String> keyMetadata, Map<String, String> tags) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String key, long size, int partNumber, String uploadID) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneInputStream readKey(String key) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneKeyDetails getKey(String key) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneKey headObject(String key) {
    throwUnsupported();

    return null;
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix) throws IOException {
    return listKeys( keyPrefix, null);
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey) throws IOException {
    return listKeys( keyPrefix, prevKey, false);
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey, boolean shallow) throws IOException {
    return proxy.listKeys(getVolumeName(), getName(), keyPrefix, prevKey, Integer.MAX_VALUE).iterator();
  }

  @Override
  public void deleteKey(String key) {
    throwUnsupported();
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(List<String> keyList, boolean quiet) {
    throwUnsupported();

    return null;
  }

  @Override
  public void renameKey(String fromKeyName, String toKeyName) {
    throwUnsupported();
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName, ReplicationConfig repConfig) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName, ReplicationConfig config, Map<String, String> metadata,
      Map<String, String> tags) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneOutputStream createMultipartKey(String key, long size, int partNumber, String uploadID) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String key, String uploadID,
      Map<Integer, String> partsMap) {
    throwUnsupported();

    return null;
  }

  @Override
  public void abortMultipartUpload(String keyName, String uploadID) {
    throwUnsupported();
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String key, String uploadID, int partNumberMarker, int maxParts) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneAcl> getAcls() {
    throwUnsupported();

    return null;
  }

  @Override
  public boolean removeAcl(OzoneAcl removeAcl) {
    throwUnsupported();

    return false;
  }

  @Override
  public boolean addAcl(OzoneAcl addAcl) {
    throwUnsupported();

    return false;
  }

  @Override
  public boolean setAcl(List<OzoneAcl> acls) {
    throwUnsupported();

    return false;
  }

  @Override
  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  @Override
  public ReplicationConfig getReplicationConfig() {
    return this.replicationConfig;
  }

  @Override
  public void createDirectory(String keyName) {
    throwUnsupported();
  }

  private static void throwUnsupported() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Inner builder for OzoneBucketStub.
   */
  public static final class Builder extends OzoneBucket.Builder {
    private final ClientProtocol proxy;

    private Builder(ClientProtocol proxy) {
      this.proxy = proxy;
    }

    @Override
    public OzoneBucketStub build() {
      return new OzoneBucketStub(this, proxy);
    }
  }
}
