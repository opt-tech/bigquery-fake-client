package jp.ne.opt.bigqueryfake

import java.sql.{JDBCType, Timestamp}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.cloud.bigquery._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RowInserter(fakeBigQuery: FakeBigQuery, tableId: TableId) {
  val fakeTable = FakeTable(fakeBigQuery, tableId)
  val table = fakeTable.get.getOrElse(throw new BigQueryException(404, s"Table not found: ${fakeTable.tableName}"))
  val tableDefinition = table.getDefinition[StandardTableDefinition]
  val partitioned = tableDefinition.getTimePartitioning != null &&
    tableDefinition.getTimePartitioning.getType == TimePartitioning.Type.DAY

  def insert(rows: Seq[Map[String, Any]]): Int = {
    if (fakeTable.partitionAwareness.partition.isDefined && !partitioned)
      throw new BigQueryException(400, s"Partition decorator is given for non-partitioned table: ${fakeTable.tableName}")

    val fields = tableDefinition.getSchema.getFields.asScala
    val columns = fields.map(_.getName) ++ (if (partitioned) Seq("_PARTITIONTIME") else Nil)
    val preparedStatement = fakeBigQuery.conn.prepareStatement(
      s"""
         |INSERT INTO ${fakeTable.datasetName}.${fakeTable.tableName}
         |(${columns.mkString(",")})
         |VALUES (${columns.map(_ => "?").mkString(",")});
         |""".stripMargin)
    rows.foreach { row =>
      fields.zipWithIndex.foreach {
        case (field, i) =>
          (row.get(field.getName), FieldType.of(field.getType)) match {
            case (Some(value), fieldType) =>
              fieldType.bind(value.toString, preparedStatement, i + 1)
            case (None, _) => preparedStatement.setNull(i + 1, java.sql.Types.NULL)
          }
      }
      if (partitioned) {
        fakeTable.partitionAwareness.partition match {
          case Some(p) =>
            preparedStatement.setTimestamp(fields.length + 1,
              Timestamp.valueOf(LocalDate.parse(p, DateTimeFormatter.ofPattern("uuuuMMdd")).atStartOfDay()))
          case None =>
            preparedStatement.setNull(fields.length + 1, java.sql.Types.TIMESTAMP)
        }
      }
      preparedStatement.addBatch()
    }
    val result = preparedStatement.executeBatch()
    preparedStatement.close()
    result.reduce(_ + _)
  }
}

