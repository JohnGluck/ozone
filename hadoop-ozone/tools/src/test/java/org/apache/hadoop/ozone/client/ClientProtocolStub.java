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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.storage.ContainerStorageStub;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

/**
 * ClientProtocol implementation with in-memory state.
 */
public class ClientProtocolStub implements ClientProtocol {
  private static final String STUB_KERBEROS_ID = "stub_kerberos_id";

  private static final String STUB_SECRET = "stub_secret";

  private final OzoneManagerProtocol ozoneManagerClient;

  private final ContainerStorageStub containerStorageStub = new ContainerStorageStub();

  public ClientProtocolStub() {
    this.ozoneManagerClient = new OzoneManagerClientStub();
  }

  @Override
  public List<OzoneManagerProtocolProtos.OMRoleInfo> getOmRoleInfos() {
    throwUnsupported();
    return null;
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
    VolumeArgs defaultArgs = VolumeArgs.newBuilder()
        .setAdmin("root")
        .setOwner("root")
        .setQuotaInBytes(Integer.MAX_VALUE)
        .build();

    createVolume(volumeName, defaultArgs);
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs args)
      throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(args.getAdmin())
        .setOwnerName(args.getOwner())
        .setQuotaInBytes(args.getQuotaInBytes())
        .setQuotaInNamespace(args.getQuotaInNamespace())
        .setCreationTime(Time.now())
        .build();

    ozoneManagerClient.createVolume(volumeArgs);
  }

  @Override
  public boolean setVolumeOwner(String volumeName, String owner) {
    throwUnsupported();
    return false;
  }

  @Override
  public void setVolumeQuota(String volumeName, long quotaInNamespace, long quotaInBytes) {
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = ozoneManagerClient.getVolumeInfo(volumeName);

    return createVolumeObject(volumeArgs);
  }

  @Override
  public S3VolumeContext getS3VolumeContext() {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneKey headS3Object(String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName, int partNumber) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneVolume buildOzoneVolume(OmVolumeArgs volume) {
    throwUnsupported();

    return null;
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl) {
    throwUnsupported();

    return false;
  }

  @Override
  public void deleteVolume(String volumeName) {
    throwUnsupported();
  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume, int maxListResult) throws IOException {
    return listVolumes(UserGroupInformation.getCurrentUser().getUserName(), volumePrefix, prevVolume, maxListResult);
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix, String prevVolume, int maxListResult)
      throws IOException {
    return ozoneManagerClient.listAllVolumes(volumePrefix, prevVolume, maxListResult)
        .stream()
        .map(this::createVolumeObject)
        .collect(toList());
  }

  private OzoneVolume createVolumeObject(OmVolumeArgs volumeArgs) {
    return OzoneVolumeStub.newBuilder(this)
        .setName(volumeArgs.getVolume())
        .setOwner(volumeArgs.getOwnerName())
        .setAdmin(volumeArgs.getAdminName())
        .setAcls(volumeArgs.getAcls())
        .setCreationTime(volumeArgs.getCreationTime())
        .setModificationTime(volumeArgs.getModificationTime())
        .setQuotaInBytes(volumeArgs.getQuotaInBytes())
        .setMetadata(volumeArgs.getMetadata())
        .setRefCount(volumeArgs.getRefCount())
        .setUsedNamespace(volumeArgs.getUsedNamespace())
        .build();
  }

  @Override
  public void createBucket(String volumeName, String bucketName) throws IOException {
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .build();

    createBucket(volumeName, bucketName, bucketArgs);
  }

  @Override
  public void createBucket(String volumeName, String bucketName, BucketArgs bucketArgs) throws IOException {
    DefaultReplicationConfig defaultReplicationConfig =
        new DefaultReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setDefaultReplicationConfig(defaultReplicationConfig)
        .setBucketLayout(bucketArgs.getBucketLayout())
        .setStorageType(bucketArgs.getStorageType())
        .setIsVersionEnabled(bucketArgs.getVersioning())
        .setCreationTime(Time.now())
        .build();

    ozoneManagerClient.createBucket(bucketInfo);
  }

  @Override
  public void setBucketVersioning(String volumeName, String bucketName, Boolean versioning) {
    throwUnsupported();
  }

  @Override
  public void setBucketStorageType(String volumeName, String bucketName, StorageType storageType) {
    throwUnsupported();
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName) {
    throwUnsupported();
  }

  @Override
  public void checkBucketAccess(String volumeName, String bucketName) {
    throwUnsupported();
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName) throws IOException {
    OmBucketInfo bucketInfo = ozoneManagerClient.getBucketInfo(volumeName, bucketName);

    return createBucketObject(bucketInfo);
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix, String prevBucket,
      int maxListResult, boolean hasSnapshot) throws IOException {
    return ozoneManagerClient.listBuckets(volumeName, null, null, maxListResult, hasSnapshot)
        .stream()
        .map(this::createBucketObject)
        .collect(toList());
  }

  private OzoneBucket createBucketObject(OmBucketInfo bucketInfo) {
    return OzoneBucketStub.newBuilder(this)
        .setVolumeName(bucketInfo.getVolumeName())
        .setName(bucketInfo.getBucketName())
        .setStorageType(bucketInfo.getStorageType())
        .setSourceBucket(bucketInfo.getSourceBucket())
        .setBucketLayout(bucketInfo.getBucketLayout())
        .setCreationTime(bucketInfo.getCreationTime())
        .setModificationTime(bucketInfo.getModificationTime())
        .setMetadata(bucketInfo.getMetadata())
        .setDefaultReplicationConfig(bucketInfo.getDefaultReplicationConfig())
        .setEncryptionKeyName(Optional.ofNullable(bucketInfo.getEncryptionKeyInfo())
            .map(BucketEncryptionKeyInfo::getKeyName)
            .orElse(null))
        .setVersioning(bucketInfo.getIsVersionEnabled())
        .setUsedNamespace(bucketInfo.getUsedNamespace())
        .setQuotaInBytes(bucketInfo.getQuotaInBytes())
        .setQuotaInNamespace(bucketInfo.getQuotaInNamespace())
        .setUsedBytes(bucketInfo.getUsedBytes())
        .build();
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, Map<String, String> metadata) throws IOException {
    return createKey(
        volumeName,
        bucketName,
        keyName,
        size,
        ReplicationConfig.fromTypeAndFactor(type, factor),
        metadata
    );
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata) throws IOException {
    return createKey(volumeName, bucketName, keyName, size, replicationConfig, metadata, emptyMap());
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) throws IOException {
    OmKeyArgs keyArgsBuilder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setReplicationConfig(replicationConfig)
        .addAllMetadataGdpr(metadata)
        .addAllTags(tags)
        .setAcls(emptyList())
        .setLatestVersionLocation(false)
        .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
        .build();

    OpenKeySession openKey = ozoneManagerClient.openKey(keyArgsBuilder);

    return createOutputStream(openKey);
  }

  private OzoneOutputStream createOutputStream(OpenKeySession openKey) {
    ByteArrayOutputStream byteArrayOutputStream = new StubOutputStream(openKey,  containerStorageStub);

    return new OzoneOutputStream(byteArrayOutputStream, null);
  }

  @Override
  public OzoneOutputStream rewriteKey(String volumeName, String bucketName, String keyName, long size,
      long existingKeyGeneration, ReplicationConfig replicationConfig, Map<String, String> metadata) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneInputStream getKey(String volumeName, String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName, boolean recursive) {
    throwUnsupported();
  }

  @Override
  public void deleteKeys(String volumeName, String bucketName, List<String> keyNameList) {
    throwUnsupported();
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(String volumeName, String bucketName, List<String> keyNameList,
      boolean quiet) {
    throwUnsupported();

    return new HashMap<>();
  }

  @Override
  public void renameKey(String volumeName, String bucketName, String fromKeyName, String toKeyName) {
    throwUnsupported();
  }

  @Override
  public void renameKeys(String volumeName, String bucketName, Map<String, String> keyMap) {
    throwUnsupported();
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName, String keyPrefix, String prevKey,
      int maxListResult) throws IOException {
    return ozoneManagerClient.listKeys(volumeName, bucketName, null, keyPrefix, maxListResult)
        .getKeys()
        .stream()
        .map(OzoneKey::fromKeyInfo)
        .collect(toList());
  }

  @Override
  public OzoneKeyDetails getKeyDetails(String volumeName, String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public void close() {
    throwUnsupported();
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationType type, ReplicationFactor factor) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig, Map<String, String> metadata) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneOutputStream createMultipartKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String volumeName, String bucketName, String keyName,
      String uploadID, Map<Integer, String> partsMap) {
    throwUnsupported();

    return null;
  }

  @Override
  public void abortMultipartUpload(String volumeName, String bucketName, String keyName, String uploadID) {
    throwUnsupported();
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String volumeName, String bucketName, String keyName,
      String uploadID, int partNumberMarker, int maxParts) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneMultipartUploadList listMultipartUploads(String volumeName, String bucketName, String prefix) {
    throwUnsupported();

    return null;
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer) {
    throwUnsupported();

    return null;
  }

  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token) {
    throwUnsupported();

    return 0;
  }

  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token) {
    throwUnsupported();
  }

  @Override
  @Nonnull
  public S3SecretValue getS3Secret(String kerberosID) {
    throwUnsupported();

    return S3SecretValue.of(STUB_KERBEROS_ID, STUB_SECRET);
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist) {
    throwUnsupported();

    return null;
  }

  @Override
  public S3SecretValue setS3Secret(String accessId, String secretKey) {
    throwUnsupported();

    return null;
  }

  @Override
  public void revokeS3Secret(String kerberosID) {
    throwUnsupported();
  }

  @Override
  public void createTenant(String tenantId) {
    throwUnsupported();
  }

  @Override
  public void createTenant(String tenantId, TenantArgs tenantArgs) {
    throwUnsupported();
  }

  @Override
  public DeleteTenantState deleteTenant(String tenantId) {
    throwUnsupported();

    return null;
  }

  @Override
  public S3SecretValue tenantAssignUserAccessId(String username, String tenantId, String accessId) {
    throwUnsupported();

    return null;
  }

  @Override
  public void tenantRevokeUserAccessId(String accessId) {
    throwUnsupported();
  }

  @Override
  public void tenantAssignAdmin(String accessId, String tenantId, boolean delegated) {
    throwUnsupported();
  }

  @Override
  public void tenantRevokeAdmin(String accessId, String tenantId) {
    throwUnsupported();
  }

  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal) {
    throwUnsupported();

    return null;
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix) {
    throwUnsupported();

    return null;
  }

  @Override
  public TenantStateList listTenant() {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneFsServerDefaults getServerDefaults() {
    throwUnsupported();

    return null;
  }

  @Override
  public KeyProvider getKeyProvider() {
    throwUnsupported();

    return null;
  }

  @Override
  public URI getKeyProviderUri() {
    throwUnsupported();

    return null;
  }

  @Override
  public String getCanonicalServiceName() {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName, String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public void createDirectory(String volumeName, String bucketName, String keyName) {
    throwUnsupported();
  }

  @Override
  public OzoneInputStream readFile(String volumeName, String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, boolean overWrite, boolean recursive) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite, boolean recursive) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName, String keyName, boolean recursive,
      String startKey, long numEntries) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName, String keyName, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(String volumeName, String bucketName, String keyName,
      boolean recursive, String startKey, long numEntries, boolean allowPartialPrefixes) {
    throwUnsupported();

    return null;
  }

  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) {
    throwUnsupported();

    return false;
  }

  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) {
    throwUnsupported();

    return false;
  }

  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) {
    throwUnsupported();

    return false;
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneManagerProtocol getOzoneManagerClient() {
    return ozoneManagerClient;
  }

  @Override
  public void setBucketQuota(String volumeName, String bucketName, long quotaInNamespace, long quotaInBytes) {
    throwUnsupported();
  }

  @Override
  public void setReplicationConfig(String volumeName, String bucketName, ReplicationConfig replicationConfig) {
    throwUnsupported();
  }

  @Deprecated
  @Override
  public void setEncryptionKey(String volumeName, String bucketName, String bekName) {
    throwUnsupported();
  }

  @Override
  public OzoneKey headObject(String volumeName, String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public void setThreadLocalS3Auth(S3Auth s3Auth) {
    throwUnsupported();
  }

  @Override
  public void setIsS3Request(boolean isS3Request) {
    throwUnsupported();
  }

  @Override
  public S3Auth getThreadLocalS3Auth() {
    throwUnsupported();

    return null;
  }

  @Override
  public void clearThreadLocalS3Auth() {
    throwUnsupported();
  }

  @Override
  public boolean setBucketOwner(String volumeName, String bucketName, String owner) {
    throwUnsupported();

    return false;
  }

  @Override
  public Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> getKeysEveryReplicas(String volumeName,
      String bucketName, String keyName) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConf, boolean overWrite, boolean recursive) {
    throwUnsupported();

    return null;
  }

  @Override
  public String createSnapshot(String volumeName, String bucketName, String snapshotName) {
    throwUnsupported();

    return "";
  }

  @Override
  public void renameSnapshot(String volumeName, String bucketName, String snapshotOldName, String snapshotNewName) {
    throwUnsupported();
  }

  @Override
  public ListSnapshotResponse listSnapshot(String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) {
    throwUnsupported();

    return null;
  }

  @Override
  public void deleteSnapshot(String volumeName, String bucketName, String snapshotName) {
    throwUnsupported();
  }

  @Override
  public OzoneSnapshot getSnapshotInfo(String volumeName, String bucketName, String snapshotName) {
    throwUnsupported();

    return null;
  }

  @Override
  public String printCompactionLogDag(String fileNamePrefix, String graphType) {
    throwUnsupported();

    return null;
  }

  @Override
  public SnapshotDiffResponse snapshotDiff(String volumeName, String bucketName, String fromSnapshot, String toSnapshot,
      String token, int pageSize, boolean forceFullDiff, boolean disableNativeDiff) {
    throwUnsupported();

    return null;
  }

  @Override
  public CancelSnapshotDiffResponse cancelSnapshotDiff(String volumeName, String bucketName, String fromSnapshot,
      String toSnapshot) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneSnapshotDiff> listSnapshotDiffJobs(String volumeName, String bucketName, String jobStatus,
      boolean listAll) {
    throwUnsupported();

    return null;
  }

  @Override
  public void setTimes(OzoneObj obj, String keyName, long mtime, long atime) {
    throwUnsupported();
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName, String keyName, boolean force) {
    throwUnsupported();

    return null;
  }

  @Override
  public void recoverKey(OmKeyArgs args, long clientID) {
    throwUnsupported();
  }

  private static void throwUnsupported() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
