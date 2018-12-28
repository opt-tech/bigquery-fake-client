package jp.ne.opt.bigqueryfake

import org.scalatest.fixture

class FakeBigQueryTest extends fixture.FunSpec with DBFixture {
  it("can be instantiated") { conn =>
    new FakeBigQuery("example", conn, new FakeStorage)
  }
}
