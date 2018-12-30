package com.google.cloud.bigquery

import com.google.cloud.RetryOption

import scala.collection.JavaConverters._

object FakeBuilder {
  def newDatasetBuilder(bigQuery: BigQuery, datasetId: DatasetId): Dataset.Builder =
    new Dataset.Builder(bigQuery, datasetId)

  def newInsertAllResponse(errors: Map[Long, Seq[BigQueryError]]): InsertAllResponse =
    new InsertAllResponse(errors.map { case (key, value) =>
        new java.lang.Long(key) -> value.asJava
    }.asJava)

  def newTableBuilder(bigquery: BigQuery, tableId: TableId, definition: TableDefinition): Table.Builder =
    new Table.Builder(bigquery, tableId, definition)
}

