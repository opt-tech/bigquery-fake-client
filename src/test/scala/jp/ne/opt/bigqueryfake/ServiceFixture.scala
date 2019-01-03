package jp.ne.opt.bigqueryfake

import java.sql.DriverManager

import org.scalatest.{Outcome, fixture}

trait ServiceFixture { self: fixture.TestSuite =>

  type FixtureParam = FakeBigQuery

  val conn = sys.env.get("POSTGRES_URL").map { url =>
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url)
  }

  override def withFixture(test: OneArgTest): Outcome = {
    val options = FakeBigQueryOptions.newBuilder
    conn.foreach(options.setConnection)
    try {
      conn.foreach(_.setAutoCommit(false))
      test(new FakeBigQuery(options.build()))
    } finally {
      conn.foreach(_.rollback())
    }
  }
}
