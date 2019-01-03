package jp.ne.opt.bigqueryfake

import java.sql.Connection

class QueryHelper(val conn: Connection) {
  def execute(sql: String): Unit = {
    val statement = conn.createStatement()
    statement.executeUpdate(sql)
    statement.close()
  }

  def get(sql: String): scala.Option[String] = {
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sql)
    val result = if (resultSet.next())
      Some(resultSet.getObject(1, classOf[String]))
    else
      None
    resultSet.close()
    statement.close()
    result
  }

  def list(sql: String): Seq[String] = {
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sql)
    val result = Iterator.continually { resultSet }.takeWhile { _.next() }.map { row =>
      row.getObject(1, classOf[String])
    }.toList
    resultSet.close()
    statement.close()
    result
  }

  def listValues(sql: String): Seq[Seq[String]] = {
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sql)
    val columnCount = resultSet.getMetaData.getColumnCount
    val result = Iterator.continually { resultSet }.takeWhile { _.next() }.map { row =>
      1 to columnCount map { i =>
        Option(row.getObject(i)).map(_.toString).orNull
      }
    }.toList
    resultSet.close()
    statement.close()
    result
  }

  def useCompatibleOneOf(h2Statement: String, postgresStatement: String): String =
    conn match {
      case _: org.h2.jdbc.JdbcConnection => h2Statement
      case _: org.postgresql.PGConnection => postgresStatement
      case _ => throw new UnsupportedOperationException(s"Unsupported JDBC connection: $conn")
    }
}
