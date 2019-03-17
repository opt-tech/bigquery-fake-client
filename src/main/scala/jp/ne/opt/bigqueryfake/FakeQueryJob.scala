package jp.ne.opt.bigqueryfake

import java.sql.ResultSet
import java.time.ZoneOffset
import java.util.Base64

import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import jp.ne.opt.bigqueryfake.rewriter.QueryRewriter

import scala.collection.JavaConverters._

class FakeQueryJob(fakeBigQuery: FakeBigQuery, config: QueryJobConfiguration) {
  def create(): Job = {
    FakeBuilder.newFakeJob(this.fakeBigQuery, JobInfo.of(config), Some(fetchResult()))
  }

  def fetchResult(): TableResult = {
    val (schema, fieldValues) = query()
    new TableResult(schema, fieldValues.length, new PageImpl[FieldValueList](null, null, fieldValues.asJava))
  }

  private def query(): (Schema, Seq[FieldValueList]) = {
    val query = new QueryRewriter(fakeBigQuery.options.rewriteMode).rewrite(config.getQuery)

    val preparedStatement = fakeBigQuery.conn.prepareStatement(query)
    config.getPositionalParameters.asScala.zipWithIndex.foreach {
      case (queryValue, i) =>
        FieldType.of(queryValue.getType).bind(queryValue.getValue, preparedStatement, i + 1)
    }
    val resultSet = preparedStatement.executeQuery()
    val metaData = resultSet.getMetaData

    val schema = Schema.of(1.to(metaData.getColumnCount).map { c =>
      Field.of(metaData.getColumnName(c), FieldType.of(metaData.getColumnType(c)).bigQueryType)
    }.asJava)
    val fieldValues = extractValues(resultSet, metaData.getColumnCount)
    resultSet.close()
    preparedStatement.close()
    schema -> fieldValues
  }

  private def extractValues(resultSet: ResultSet, columnCount: Int): Seq[FieldValueList] = {
    Iterator.continually { resultSet }.takeWhile { _.next() }.map { row =>
      FieldValueList.of(1.to(columnCount).map { c =>
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, row.getObject(c) match {
          case t: java.sql.Timestamp =>
            val instant = t.toLocalDateTime.toInstant(ZoneOffset.UTC)
            "%d.%06d".format(instant.getEpochSecond, instant.getNano / 1000)
          case d: java.sql.Date =>
            d.toString
          case t: java.sql.Time =>
            t.toString
          case b: Array[Byte] => new String(Base64.getEncoder.encode(b))
          case o => o
        })
      }.toList.asJava)
    }.toList

  }
}

