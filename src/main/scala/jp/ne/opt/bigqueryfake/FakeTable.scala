package jp.ne.opt.bigqueryfake

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{BigQueryException, DatasetId, FakeBuilder, Field, Schema, StandardTableDefinition, Table, TableDefinition, TableId, TableInfo, TimePartitioning}

import scala.collection.JavaConverters._
import scala.util.Try

case class FakeTable(fakeBigQuery: FakeBigQuery, tableId: TableId) {

  case class PartitionAwareTableId(tableId: String) {
    val tableParts: Seq[String] = tableId.split("\\$", 2)

    def tableName: String = tableParts.head

    def partition: scala.Option[String] = tableParts.lift(1)
  }

  val datasetName: String = tableId.getDataset
  val tableName: String = partitionAwareness.tableName

  lazy val partitionAwareness = PartitionAwareTableId(tableId.getTable)

  def create(tableDefinition: TableDefinition): Table = {
    val partitioning = tableDefinition match {
      case d: StandardTableDefinition if d.getTimePartitioning != null =>
         d.getTimePartitioning.getType == TimePartitioning.Type.DAY
      case _ => false
    }
    val columns = tableDefinition.getSchema.getFields.asScala.map { field =>
      val fieldType = FieldType.of(field.getType)
      s"${field.getName} ${fieldType.sqlType} ${if (field.getMode == Mode.REQUIRED) "NOT NULL" else "NULL"}"
    } ++ (if (partitioning) Seq(s"${FakeTable.PartitionColumnName} timestamp NOT NULL") else Nil)

    val statment =
      s"""
       |CREATE TABLE IF NOT EXISTS $datasetName.$tableName (
       |${columns.mkString(",")}
       |)
       """.stripMargin
    fakeBigQuery.queryHelper.execute(statment)
    get.getOrElse(throw new BigQueryException(404, s"Failed to create table: $tableName"))
  }

  def delete(): Boolean =
    Try(fakeBigQuery.queryHelper.execute(s"DROP TABLE $datasetName.$tableName;")).isSuccess

  def get: Option[Table] =
    fakeBigQuery.queryHelper.get(s"SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema = '$datasetName' AND table_name = '$tableName';")
      .map { _ =>
        val metaData = fakeBigQuery.conn.getMetaData
        val resultSet = metaData.getColumns(null, datasetName, tableName, "%")
        val fields = Iterator.continually { resultSet }.takeWhile { _.next() }.map { rs =>
          rs.getObject(1, classOf[String])
          Field.of(rs.getString("COLUMN_NAME"), FieldType.of(rs.getInt("DATA_TYPE")).bigQueryType)
        }.toList
        val partitioned = fields.exists(_.getName.toUpperCase == FakeTable.PartitionColumnName)
        val fieldsWithoutPartitionColumn = fields.filterNot(_.getName.toUpperCase == FakeTable.PartitionColumnName)
        val definitionBuilder = StandardTableDefinition.newBuilder().setSchema(Schema.of(fieldsWithoutPartitionColumn.asJava))
        if (partitioned)
          definitionBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
        FakeBuilder.newTableBuilder(fakeBigQuery, TableId.of(datasetName, tableName), definitionBuilder.build).build()
      }

  def update(): Table = // Update operation is not supported, do nothing
    get.getOrElse(throw new BigQueryException(404, s"Dataset not found: ${tableName}"))
}

object FakeTable {
  val PartitionColumnName: String = "_PARTITIONTIME"

  def list(fakeBigQuery: FakeBigQuery, datasetId: DatasetId): Seq[Table] =
    fakeBigQuery.queryHelper
      .list(s"SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema = '${datasetId.getDataset}';")
      .flatMap(table => FakeTable(fakeBigQuery, TableId.of(datasetId.getDataset, table)).get)
}