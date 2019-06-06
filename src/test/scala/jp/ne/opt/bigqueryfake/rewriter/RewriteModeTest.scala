package jp.ne.opt.bigqueryfake.rewriter

import org.scalatest.{FunSpec, MustMatchers}

class RewriteModeTest extends FunSpec with MustMatchers {
  describe("valueOf") {
    it("can detect H2") {
      RewriteMode.valueOf("h2") mustBe RewriteMode.H2
      RewriteMode.valueOf("H2") mustBe RewriteMode.H2
    }

    it("can detect PostgreSQL") {
      RewriteMode.valueOf("postgres") mustBe RewriteMode.PostgreSQL
      RewriteMode.valueOf("postgresql") mustBe RewriteMode.PostgreSQL
      RewriteMode.valueOf("Postgres") mustBe RewriteMode.PostgreSQL
      RewriteMode.valueOf("PostgreSQL") mustBe RewriteMode.PostgreSQL
    }

    it("throws error with unknown rewrite mode") {
      an [IllegalArgumentException] mustBe thrownBy(RewriteMode.valueOf("postgre"))
      an [IllegalArgumentException] mustBe thrownBy(RewriteMode.valueOf("postgressql"))
      an [IllegalArgumentException] mustBe thrownBy(RewriteMode.valueOf("H3"))
      an [IllegalArgumentException] mustBe thrownBy(RewriteMode.valueOf("h"))
    }
  }
}

