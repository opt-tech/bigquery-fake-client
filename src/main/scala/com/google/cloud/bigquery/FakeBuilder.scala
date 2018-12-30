package com.google.cloud.bigquery

import com.google.cloud.RetryOption

import scala.collection.JavaConverters._

object FakeBuilder {
  def newDatasetBuilder(bigQuery: BigQuery, datasetId: DatasetId): Dataset.Builder =
    new Dataset.Builder(bigQuery, datasetId)

  def newFakeJob(bigQuery: BigQuery, jobInfo: JobInfo, queryResults: scala.Option[TableResult] = None): Job =
    new Job(bigQuery, new JobInfo.BuilderImpl(jobInfo).setStatus(new JobStatus(JobStatus.State.DONE)).asInstanceOf[JobInfo.BuilderImpl]) {
      override def isDone: Boolean = true

      override def waitFor(waitOptions: RetryOption*): Job = {
        // Do nothing, because all queries are synchronous in JDBC
        this
      }

      override def getQueryResults(options: BigQuery.QueryResultsOption*): TableResult =
        queryResults.getOrElse(super.getQueryResults(options: _*))
    }

  def newInsertAllResponse(errors: Map[Long, Seq[BigQueryError]]): InsertAllResponse =
    new InsertAllResponse(errors.map { case (key, value) =>
        new java.lang.Long(key) -> value.asJava
    }.asJava)

  def newTableBuilder(bigquery: BigQuery, tableId: TableId, definition: TableDefinition): Table.Builder =
    new Table.Builder(bigquery, tableId, definition)
}

