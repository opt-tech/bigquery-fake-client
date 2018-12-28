package com.google.cloud.storage

object FakeBuilder {
  def buildTargetOptions(options: Seq[Storage.BlobWriteOption]): Seq[Storage.BlobTargetOption] =
    options.map(_.toTargetOption)

  def newBlob(storage: Storage, blobInfo: BlobInfo): Blob = new Blob(storage, new BlobInfo.BuilderImpl(blobInfo))
}
