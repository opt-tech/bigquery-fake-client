package jp.ne.opt.bigqueryfake

import java.sql.{DriverManager, Connection}
import java.util.Properties

import org.scalatest.{Outcome, fixture}

trait DBFixture { self: fixture.TestSuite =>

  type FixtureParam = Connection

  override def withFixture(test: OneArgTest): Outcome = {
    val url = "jdbc:h2:mem:;MODE=PostgreSQL;DATABASE_TO_UPPER=false"
    val prop = new Properties()
    prop.setProperty("user", "sa")
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, prop)
    try {
      test(conn)
    } finally {
      conn.close()
    }
  }
}
