package jp.ne.opt.bigqueryfake

import com.google.cloud.bigquery.{BigQueryException, Dataset, DatasetId, FakeBuilder}

import scala.util.Try

case class FakeDataset(fakeBigQuery: FakeBigQuery, datasetId: DatasetId) {
  val datasetName = datasetId.getDataset

  def create(): Dataset = {
    fakeBigQuery.queryHelper.execute(s"CREATE SCHEMA IF NOT EXISTS ${datasetName};")
    get.getOrElse(throw new BigQueryException(404, s"Failed to create dataset: ${datasetName}"))
  }

  def get: Option[Dataset] =
    fakeBigQuery.queryHelper
      .get(s"SELECT schema_name FROM INFORMATION_SCHEMA.schemata WHERE schema_name = '${datasetName}';")
      .map(_ => FakeBuilder.newDatasetBuilder(fakeBigQuery, datasetId).build())


  def delete(): Boolean =
    Try(fakeBigQuery.queryHelper.execute(s"DROP SCHEMA IF EXISTS ${datasetName};")).isSuccess

  def update(): Dataset = // Update operation is not supported, do nothing
    get.getOrElse(throw new BigQueryException(404, s"Dataset not found: ${datasetName}"))
}

object FakeDataset {
  def list(fakeBigQuery: FakeBigQuery): Seq[Dataset] =
    fakeBigQuery.queryHelper
      .list(s"SELECT schema_name FROM INFORMATION_SCHEMA.schemata WHERE schema_name NOT IN ('INFORMATION_SCHEMA', 'PUBLIC');")
      .map(schema => FakeBuilder.newDatasetBuilder(fakeBigQuery, DatasetId.of(schema)).build())
}