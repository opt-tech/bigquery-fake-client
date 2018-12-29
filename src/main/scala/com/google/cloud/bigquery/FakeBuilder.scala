package com.google.cloud.bigquery

object FakeBuilder {
  def newDatasetBuilder(bigQuery: BigQuery, datasetId: DatasetId): Dataset.Builder =
    new Dataset.Builder(bigQuery, datasetId)

  def newTableBuilder(bigquery: BigQuery, tableId: TableId, definition: TableDefinition): Table.Builder =
    new Table.Builder(bigquery, tableId, definition)
}

