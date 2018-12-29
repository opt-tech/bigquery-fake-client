package jp.ne.opt.bigqueryfake

import org.scalatest.fixture

class FakeBigQuerySpec extends fixture.FunSpec with DBFixture {
  it("can be instantiated") { conn =>
    new FakeBigQuery(conn)
  }
}
