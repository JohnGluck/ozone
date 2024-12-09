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

package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import static java.lang.String.format;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

/**
 * OzoneManagerClient in-memory stub for testing.
 */
public class OzoneManagerClientStub implements OzoneManagerProtocol {
  private final Map<String, OmVolumeArgs> volumes = new HashMap<>();

  private final Map<String, OmBucketInfo> buckets = new HashMap<>();

  private final Map<String, OmKeyInfo> keys = new HashMap<>();

  private final AtomicLong sessionId = new AtomicLong(0);

  @Override
  public void createVolume(OmVolumeArgs volumeArgs) {
    String volumeName = volumeArgs.getVolume();

    volumes.put(volumeName, volumeArgs);
  }

  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws OMException {
    if (!volumes.containsKey(volume)) {
      throw new OMException(format("Volume '%s' is missing", volume), VOLUME_NOT_FOUND);
    }

    return volumes.get(volume);
  }

  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String prevKey, int maxKeys) {
    throwUnsupported();

    return Collections.emptyList();
  }

  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey, int maxKeys) {
    return new ArrayList<>(volumes.values());
  }

  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();

    buckets.put(fullBucketName(volumeName, bucketName), bucketInfo);
  }

  @Override
  public OmBucketInfo getBucketInfo(String volumeName, String bucketName) throws OMException {
    if (!buckets.containsKey(fullBucketName(volumeName, bucketName))) {
      throw new OMException(format("Bucket '%s.%s' is missing", volumeName, bucketName), BUCKET_NOT_FOUND);
    }

    return buckets.get(fullBucketName(volumeName, bucketName));
  }

  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setDataSize(args.getDataSize())
        .setFile(true)
        .setOmKeyLocationInfos(Collections.emptyList())
        .build();

    keys.put(fullKeyName(args.getVolumeName(), args.getBucketName(), args.getKeyName()), keyInfo);

    return new OpenKeySession(sessionId.incrementAndGet(), keyInfo, 0);
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) {
    throwUnsupported();

    return null;
  }

  @Override
  public KeyInfoWithVolumeContext getKeyInfo(OmKeyArgs args, boolean assumeS3Context) throws IOException {
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();

    String fullKeyName = fullKeyName(volumeName, bucketName, keyName);

    if (!keys.containsKey(fullKeyName)) {
      throw new OMException(format("Key '%s.%s.%s' is missing", volumeName, bucketName, keyName), KEY_NOT_FOUND);
    }

    OmKeyInfo keyInfo = keys.get(fullKeyName);

    OmVolumeArgs volumeArgs = volumes.get(args.getVolumeName());

    return KeyInfoWithVolumeContext.newBuilder()
        .setKeyInfo(keyInfo)
        .setVolumeArgs(volumeArgs)
        .setUserPrincipal(UserGroupInformation.getCurrentUser().getShortUserName())
        .build();
  }

  @Override
  public List<OmBucketInfo> listBuckets(String volumeName, String startBucketName, String bucketPrefix,
      int maxNumOfBuckets, boolean hasSnapshot) {
    return new ArrayList<>(buckets.values());
  }

  @Override
  public List<ServiceInfo> getServiceList() {
    throwUnsupported();

    return Collections.emptyList();
  }

  @Override
  public ServiceInfoEx getServiceInfo() {
    throwUnsupported();

    return null;
  }

  @Override
  public ListOpenFilesResult listOpenFiles(String path, int maxKeys, String contToken) {
    throwUnsupported();

    return null;
  }

  @Override
  public void transferLeadership(String newLeaderId) {
    throwUnsupported();
  }

  @Override
  public boolean triggerRangerBGSync(boolean noWait) {
    throwUnsupported();

    return false;
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID) {
    throwUnsupported();

    return null;
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(String upgradeClientID, boolean takeover,
      boolean readonly) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName, String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts) {
    throwUnsupported();

    return null;
  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName, String bucketName, String prefix) {
    throwUnsupported();

    return null;
  }

  @Override
  public S3VolumeContext getS3VolumeContext() {
    throwUnsupported();

    return null;
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
  public OmKeyInfo lookupFile(OmKeyArgs keyArgs) {
    throwUnsupported();

    return null;
  }

  @Override
  public ListKeysResult listKeys(String volumeName, String bucketName, String startKey, String keyPrefix, int maxKeys) {
    return new ListKeysResult(new ArrayList<>(keys.values()), false);
  }

  @Override
  public ListKeysLightResult listKeysLight(String volumeName, String bucketName, String startKey, String keyPrefix,
      int maxKeys) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) {
    throwUnsupported();

    return  Collections.emptyList();
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive, String startKey, long numEntries) {
    throwUnsupported();

    return  Collections.emptyList();
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs keyArgs) {
    throwUnsupported();

    return null;
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs keyArgs, boolean recursive, String startKey, long numEntries,
      boolean allowPartialPrefixes) {
    throwUnsupported();

    return  Collections.emptyList();
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(OmKeyArgs keyArgs, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) {
    throwUnsupported();

    return  Collections.emptyList();
  }

  @Override
  public DBUpdates getDBUpdates(OzoneManagerProtocolProtos.DBUpdatesRequest dbUpdatesRequest) {
    throwUnsupported();

    return null;
  }

  @Override
  public OzoneManagerProtocolProtos.EchoRPCResponse echoRPCReq(byte[] payloadReq, int payloadSizeResp,
      boolean writeToRatis) {
    throwUnsupported();

    return null;
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName, String keyName, boolean force) {
    throwUnsupported();

    return null;
  }

  @Override
  public void setTimes(OmKeyArgs keyArgs, long mtime, long atime) {
    throwUnsupported();
  }

  @Override
  public UUID refetchSecretKey() {
    throwUnsupported();

    return null;
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked) {
    throwUnsupported();

    return false;
  }

  @Override
  public String getQuotaRepairStatus() {
    throwUnsupported();

    return "";
  }

  @Override
  public void startQuotaRepair(List<String> buckets) {
    throwUnsupported();
  }

  @Override
  public void close() throws IOException {
    throwUnsupported();
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

  private void throwUnsupported() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private static String fullBucketName(String volumeName, String bucketName) {
    return volumeName + "/" + bucketName;
  }

  private static String fullKeyName(String volumeName, String bucketName, String key) {
    return volumeName + "/" + bucketName + "/" + key;
  }
}
