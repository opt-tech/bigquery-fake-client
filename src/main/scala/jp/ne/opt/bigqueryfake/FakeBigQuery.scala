package jp.ne.opt.bigqueryfake

import java.sql.Connection

import com.google.api.gax.paging.Page
import com.google.cloud.bigquery.BigQuery.JobOption
import com.google.cloud.bigquery._
import com.google.cloud.storage.Storage

class FakeBigQuery(val projectId: String, val conn: Connection, val storage: Storage) extends BigQuery {
  override def create(datasetInfo: DatasetInfo, options: BigQuery.DatasetOption*): Dataset = ???

  override def create(tableInfo: TableInfo, options: BigQuery.TableOption*): Table = ???

  override def create(jobInfo: JobInfo, options: JobOption*): Job = ???

  override def getDataset(datasetId: String, options: BigQuery.DatasetOption*): Dataset = ???

  override def getDataset(datasetId: DatasetId, options: BigQuery.DatasetOption*): Dataset = ???

  override def listDatasets(options: BigQuery.DatasetListOption*): Page[Dataset] = ???

  override def listDatasets(projectId: String, options: BigQuery.DatasetListOption*): Page[Dataset] = ???

  override def delete(datasetId: String, options: BigQuery.DatasetDeleteOption*): Boolean = ???

  override def delete(datasetId: DatasetId, options: BigQuery.DatasetDeleteOption*): Boolean = ???

  override def delete(datasetId: String, tableId: String): Boolean = ???

  override def delete(tableId: TableId): Boolean = ???

  override def update(datasetInfo: DatasetInfo, options: BigQuery.DatasetOption*): Dataset = ???

  override def update(tableInfo: TableInfo, options: BigQuery.TableOption*): Table = ???

  override def getTable(datasetId: String, tableId: String, options: BigQuery.TableOption*): Table = ???

  override def getTable(tableId: TableId, options: BigQuery.TableOption*): Table = ???

  override def listTables(datasetId: String, options: BigQuery.TableListOption*): Page[Table] = ???

  override def listTables(datasetId: DatasetId, options: BigQuery.TableListOption*): Page[Table] = ???

  override def insertAll(request: InsertAllRequest): InsertAllResponse = ???

  override def listTableData(datasetId: String, tableId: String, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(tableId: TableId, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(datasetId: String, tableId: String, schema: Schema, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(tableId: TableId, schema: Schema, options: BigQuery.TableDataListOption*): TableResult = ???

  override def getJob(jobId: String, options: JobOption*): Job = ???

  override def getJob(jobId: JobId, options: JobOption*): Job = ???

  override def listJobs(options: BigQuery.JobListOption*): Page[Job] = ???

  override def cancel(jobId: String): Boolean = ???

  override def cancel(jobId: JobId): Boolean = ???

  override def query(configuration: QueryJobConfiguration, options: JobOption*): TableResult = ???

  override def query(configuration: QueryJobConfiguration, jobId: JobId, options: JobOption*): TableResult = ???

  override def getQueryResults(jobId: JobId, options: BigQuery.QueryResultsOption*): QueryResponse = ???

  override def writer(writeChannelConfiguration: WriteChannelConfiguration): TableDataWriteChannel = ???

  override def writer(jobId: JobId, writeChannelConfiguration: WriteChannelConfiguration): TableDataWriteChannel = ???

  override def getOptions: BigQueryOptions = ???
}
