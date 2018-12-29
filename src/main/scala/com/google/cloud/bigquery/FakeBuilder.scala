package com.google.cloud.bigquery

object FakeBuilder {
  def newDatasetBuilder(bigQuery: BigQuery, datasetId: DatasetId): Dataset.Builder =
    new Dataset.Builder(bigQuery, datasetId)
}

