package jp.ne.opt.bigqueryfake

import java.sql.{Connection, DriverManager}

import org.scalatest.{Informing, Outcome, fixture}

trait ServiceFixture { self: fixture.TestSuite with Informing =>

  type FixtureParam = FakeBigQuery

  val conn: Option[Connection] = sys.env.get("POSTGRES_URL").map { url =>
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url)
  }
  conn match {
    case Some(_) => info("Running tests using PostgreSQL")
    case _ => info("Running tests using H2")
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
