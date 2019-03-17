package jp.ne.opt.bigqueryfake

import java.sql.Connection

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.BigQuery.JobOption
import com.google.cloud.bigquery._
import com.google.cloud.storage.Storage

import scala.collection.JavaConverters._

class FakeBigQuery(val options: FakeBigQueryOptions) extends BigQuery {
  val conn: Connection = options.connection
  val storage: Storage = options.storage

  override def create(datasetInfo: DatasetInfo, options: BigQuery.DatasetOption*): Dataset =
    FakeDataset(this, datasetInfo.getDatasetId).create()

  override def create(tableInfo: TableInfo, options: BigQuery.TableOption*): Table =
    FakeTable(this, tableInfo.getTableId).create(tableInfo.getDefinition[TableDefinition])

  override def create(jobInfo: JobInfo, options: BigQuery.JobOption*): Job =
    jobInfo.getConfiguration[JobConfiguration] match {
      case config: LoadJobConfiguration =>
        new FakeLoadJob(this, config).create()
      case config: QueryJobConfiguration =>
        new FakeQueryJob(this, config).create()
      case jobConfig => throw new UnsupportedOperationException(s"Unsupported job configuration type: $jobConfig")
    }

  override def getDataset(datasetId: String, options: BigQuery.DatasetOption*): Dataset =
    getDataset(DatasetId.of(datasetId), options: _*)

  override def getDataset(datasetId: DatasetId, options: BigQuery.DatasetOption*): Dataset =
    FakeDataset(this, datasetId).get.orNull

  override def listDatasets(options: BigQuery.DatasetListOption*): Page[Dataset] =
    new PageImpl[Dataset](null, null, FakeDataset.list(this).asJava)

  override def listDatasets(projectId: String, options: BigQuery.DatasetListOption*): Page[Dataset] =
    listDatasets(options: _*) // projectId is not supported

  override def delete(datasetId: String, options: BigQuery.DatasetDeleteOption*): Boolean =
    delete(DatasetId.of(datasetId), options: _*)

  override def delete(datasetId: DatasetId, options: BigQuery.DatasetDeleteOption*): Boolean =
    FakeDataset(this, datasetId).delete()

  override def delete(datasetId: String, tableId: String): Boolean =
    delete(TableId.of(datasetId, tableId))

  override def delete(tableId: TableId): Boolean =
    FakeTable(this, tableId).delete()

  override def update(datasetInfo: DatasetInfo, options: BigQuery.DatasetOption*): Dataset =
    FakeDataset(this, datasetInfo.getDatasetId).update()

  override def update(tableInfo: TableInfo, options: BigQuery.TableOption*): Table =
    FakeTable(this, tableInfo.getTableId).update()

  override def getTable(datasetId: String, tableId: String, options: BigQuery.TableOption*): Table =
    getTable(TableId.of(datasetId, tableId))

  override def getTable(tableId: TableId, options: BigQuery.TableOption*): Table =
    FakeTable(this, tableId).get.orNull

  override def listTables(datasetId: String, options: BigQuery.TableListOption*): Page[Table] =
    listTables(DatasetId.of(datasetId), options: _*)

  override def listTables(datasetId: DatasetId, options: BigQuery.TableListOption*): Page[Table] =
    new PageImpl[Table](null, null, FakeTable.list(this, datasetId).asJava)

  override def insertAll(request: InsertAllRequest): InsertAllResponse = {
    new RowInserter(this, request.getTable)
      .insert(request.getRows.asScala.map(_.getContent.asScala.toMap))
    FakeBuilder.newInsertAllResponse(Map.empty)
  }

  override def listTableData(datasetId: String, tableId: String, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(tableId: TableId, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(datasetId: String, tableId: String, schema: Schema, options: BigQuery.TableDataListOption*): TableResult = ???

  override def listTableData(tableId: TableId, schema: Schema, options: BigQuery.TableDataListOption*): TableResult = ???

  override def getJob(jobId: String, options: JobOption*): Job = ???

  override def getJob(jobId: JobId, options: JobOption*): Job = ???

  override def listJobs(options: BigQuery.JobListOption*): Page[Job] = ???

  override def cancel(jobId: String): Boolean = ???

  override def cancel(jobId: JobId): Boolean = ???

  override def query(configuration: QueryJobConfiguration, options: JobOption*): TableResult =
    query(configuration, null: JobId, options: _*)

  override def query(configuration: QueryJobConfiguration, jobId: JobId, options: JobOption*): TableResult =
    new FakeQueryJob(this, configuration).fetchResult()

  override def getQueryResults(jobId: JobId, options: BigQuery.QueryResultsOption*): QueryResponse = ???

  override def writer(writeChannelConfiguration: WriteChannelConfiguration): TableDataWriteChannel = ???

  override def writer(jobId: JobId, writeChannelConfiguration: WriteChannelConfiguration): TableDataWriteChannel = ???

  override def getOptions: BigQueryOptions =
    BigQueryOptions.newBuilder()
      .setProjectId(options.projectId)
      .setCredentials(GoogleCredentials.newBuilder().build())
      .build()

  private[bigqueryfake] val queryHelper = new QueryHelper(conn)
}
