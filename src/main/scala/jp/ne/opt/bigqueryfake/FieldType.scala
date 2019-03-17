package jp.ne.opt.bigqueryfake

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.Types._
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import com.google.cloud.bigquery.{LegacySQLTypeName, StandardSQLTypeName}

import scala.util.Try

case class TypeMappingException(message: String) extends RuntimeException(message)

sealed abstract class FieldType(val bigQueryType: LegacySQLTypeName,
                                val sqlType: String,
                                val jdbcTypes: Seq[Int]) {
  def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit
}

trait DateTimeParser {
  protected def parseDateTime(str: String): Try[LocalDateTime] = Try {
    LocalDateTime.parse(str)
  } recover {
    case _: DateTimeParseException => LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS+00:00"))
  } recover {
    case _: DateTimeParseException => LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME)
  }
}

object FieldType {
  case object Byte extends FieldType(LegacySQLTypeName.BYTES, "BYTEA", Seq(BINARY, VARBINARY, LONGVARBINARY, BLOB)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setString(parameterIndex, rawValue)
  }
  case object String extends FieldType(LegacySQLTypeName.STRING, "VARCHAR(65536)", Seq(CHAR, VARCHAR, LONGVARCHAR, CLOB, NCHAR, NVARCHAR, LONGNVARCHAR, NCLOB)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setString(parameterIndex, rawValue)
  }
  case object Integer extends FieldType(LegacySQLTypeName.INTEGER, "BIGINT", Seq(TINYINT, SMALLINT, INTEGER, BIGINT)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setInt(parameterIndex, rawValue.toInt)
  }
  case object Float extends FieldType(LegacySQLTypeName.FLOAT, "DOUBLE PRECISION", Seq(FLOAT, REAL, DOUBLE)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setDouble(parameterIndex, rawValue.toDouble)
  }
  case object Numeric extends FieldType(LegacySQLTypeName.NUMERIC, "DECIMAL", Seq(NUMERIC, DECIMAL)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setBigDecimal(parameterIndex, new BigDecimal(rawValue))
  }
  case object Boolean extends FieldType(LegacySQLTypeName.BOOLEAN, "BOOLEAN", Seq(BIT, BOOLEAN)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setBoolean(parameterIndex, java.lang.Boolean.valueOf(rawValue))
  }
  case object Timestamp extends FieldType(LegacySQLTypeName.TIMESTAMP, "TIMESTAMP", Seq(TIMESTAMP)) with DateTimeParser {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(parseDateTime(rawValue).get))
  }
  case object Date extends FieldType(LegacySQLTypeName.DATE, "DATE", Seq(DATE)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setDate(parameterIndex, java.sql.Date.valueOf(rawValue))
  }
  case object Time extends FieldType(LegacySQLTypeName.TIME, "TIME", Seq(TIME)) {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setTime(parameterIndex, java.sql.Time.valueOf(rawValue))
  }
  case object DateTime extends FieldType(LegacySQLTypeName.DATETIME, "TIMESTAMP", Seq(TIMESTAMP)) with DateTimeParser {
    def bind(rawValue: String, statement: PreparedStatement, parameterIndex: Int): Unit = statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(parseDateTime(rawValue).get))
  }

  val values = Set(
    Byte,
    String,
    Integer,
    Float,
    Numeric,
    Boolean,
    Timestamp,
    Date,
    Time,
    DateTime
  )

  def of(legacySQLType: LegacySQLTypeName): FieldType = {
    values.find(_.bigQueryType == legacySQLType).getOrElse(throw TypeMappingException(s"Unknown LegacySQLTypeName: $legacySQLType"))
  }
  def of(standardSQLType: StandardSQLTypeName): FieldType = {
    values.find(_.bigQueryType.getStandardType == standardSQLType).getOrElse(throw TypeMappingException(s"Unknown StandardSQLTypeName: $standardSQLType"))
  }
  def of(jdbcType: Int): FieldType = {
    values.find(_.jdbcTypes.exists(_ == jdbcType)).getOrElse(throw TypeMappingException(s"Unknown JDBC type: $jdbcType"))
  }
}
