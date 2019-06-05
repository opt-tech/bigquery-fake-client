package jp.ne.opt.bigqueryfake

import java.io.InputStream
import java.net.URL
import java.util.concurrent.TimeUnit
import java.{lang, util}

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage._
import com.google.cloud.{Policy, ReadChannel, WriteChannel}
import com.google.common.io.ByteStreams

class FakeStorage extends Storage {
  val backend: collection.mutable.Map[String, Array[Byte]] = collection.concurrent.TrieMap()

  override def create(bucketInfo: BucketInfo, options: Storage.BucketTargetOption*): Bucket = ???

  override def create(blobInfo: BlobInfo, options: Storage.BlobTargetOption*): Blob = ???

  override def create(blobInfo: BlobInfo, content: Array[Byte], options: Storage.BlobTargetOption*): Blob = {
    backend.put(s"${blobInfo.getBucket}/${blobInfo.getName}", content)
    FakeBuilder.newBlob(this, blobInfo)
  }

  override def create(blobInfo: BlobInfo, content: Array[Byte], offset: Int, length: Int, options: Storage.BlobTargetOption*): Blob = ???

  override def create(blobInfo: BlobInfo, content: InputStream, options: Storage.BlobWriteOption*): Blob =
    create(blobInfo, ByteStreams.toByteArray(content), FakeBuilder.buildTargetOptions(options): _*)

  override def get(bucket: String, options: Storage.BucketGetOption*): Bucket = ???

  override def lockRetentionPolicy(bucket: BucketInfo, options: Storage.BucketTargetOption*): Bucket = ???

  override def get(bucket: String, blob: String, options: Storage.BlobGetOption*): Blob = ???

  override def get(blob: BlobId, options: Storage.BlobGetOption*): Blob = ???

  override def get(blob: BlobId): Blob = ???

  override def list(options: Storage.BucketListOption*): Page[Bucket] = ???

  override def list(bucket: String, options: Storage.BlobListOption*): Page[Blob] = ???

  override def update(bucketInfo: BucketInfo, options: Storage.BucketTargetOption*): Bucket = ???

  override def update(blobInfo: BlobInfo, options: Storage.BlobTargetOption*): Blob = ???

  override def update(blobInfo: BlobInfo): Blob = ???

  override def delete(bucket: String, options: Storage.BucketSourceOption*): Boolean = ???

  override def delete(bucket: String, blob: String, options: Storage.BlobSourceOption*): Boolean = {
    backend.remove(s"$bucket/$blob")
    true
  }

  override def delete(blob: BlobId, options: Storage.BlobSourceOption*): Boolean =
    delete(blob.getBucket, blob.getName, options: _*)

  override def delete(blob: BlobId): Boolean =
    delete(blob.getBucket, blob.getName)

  override def compose(composeRequest: Storage.ComposeRequest): Blob = ???

  override def copy(copyRequest: Storage.CopyRequest): CopyWriter = ???

  override def readAllBytes(bucket: String, blob: String, options: Storage.BlobSourceOption*): Array[Byte] =
    backend.getOrElse(s"$bucket/$blob", throw new StorageException(404, "Not Found"))

  override def readAllBytes(blob: BlobId, options: Storage.BlobSourceOption*): Array[Byte] =
    readAllBytes(blob.getBucket, blob.getName, options: _*)

  override def batch(): StorageBatch = ???

  override def reader(bucket: String, blob: String, options: Storage.BlobSourceOption*): ReadChannel = ???

  override def reader(blob: BlobId, options: Storage.BlobSourceOption*): ReadChannel = ???

  override def writer(blobInfo: BlobInfo, options: Storage.BlobWriteOption*): WriteChannel = ???

  override def writer(signedURL: URL): WriteChannel = ???

  override def signUrl(blobInfo: BlobInfo, duration: Long, unit: TimeUnit, options: Storage.SignUrlOption*): URL = ???

  override def get(blobIds: BlobId*): util.List[Blob] = ???

  override def get(blobIds: lang.Iterable[BlobId]): util.List[Blob] = ???

  override def update(blobInfos: BlobInfo*): util.List[Blob] = ???

  override def update(blobInfos: lang.Iterable[BlobInfo]): util.List[Blob] = ???

  override def delete(blobIds: BlobId*): util.List[lang.Boolean] = ???

  override def delete(blobIds: lang.Iterable[BlobId]): util.List[lang.Boolean] = ???

  override def getAcl(bucket: String, entity: Acl.Entity, options: Storage.BucketSourceOption*): Acl = ???

  override def getAcl(bucket: String, entity: Acl.Entity): Acl = ???

  override def deleteAcl(bucket: String, entity: Acl.Entity, options: Storage.BucketSourceOption*): Boolean = ???

  override def deleteAcl(bucket: String, entity: Acl.Entity): Boolean = ???

  override def createAcl(bucket: String, acl: Acl, options: Storage.BucketSourceOption*): Acl = ???

  override def createAcl(bucket: String, acl: Acl): Acl = ???

  override def updateAcl(bucket: String, acl: Acl, options: Storage.BucketSourceOption*): Acl = ???

  override def updateAcl(bucket: String, acl: Acl): Acl = ???

  override def listAcls(bucket: String, options: Storage.BucketSourceOption*): util.List[Acl] = ???

  override def listAcls(bucket: String): util.List[Acl] = ???

  override def getDefaultAcl(bucket: String, entity: Acl.Entity): Acl = ???

  override def deleteDefaultAcl(bucket: String, entity: Acl.Entity): Boolean = ???

  override def createDefaultAcl(bucket: String, acl: Acl): Acl = ???

  override def updateDefaultAcl(bucket: String, acl: Acl): Acl = ???

  override def listDefaultAcls(bucket: String): util.List[Acl] = ???

  override def getAcl(blob: BlobId, entity: Acl.Entity): Acl = ???

  override def deleteAcl(blob: BlobId, entity: Acl.Entity): Boolean = ???

  override def createAcl(blob: BlobId, acl: Acl): Acl = ???

  override def updateAcl(blob: BlobId, acl: Acl): Acl = ???

  override def listAcls(blob: BlobId): util.List[Acl] = ???

  override def getIamPolicy(bucket: String, options: Storage.BucketSourceOption*): Policy = ???

  override def setIamPolicy(bucket: String, policy: Policy, options: Storage.BucketSourceOption*): Policy = ???

  override def testIamPermissions(bucket: String, permissions: util.List[String], options: Storage.BucketSourceOption*): util.List[lang.Boolean] = ???

  override def getServiceAccount(projectId: String): ServiceAccount = ???

  override def getOptions: StorageOptions =
    StorageOptions.newBuilder()
      .setProjectId("bigqueryfake")
      .setCredentials(GoogleCredentials.newBuilder().build())
      .build()
}
