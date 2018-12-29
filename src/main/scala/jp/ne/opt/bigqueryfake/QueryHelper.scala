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
}
