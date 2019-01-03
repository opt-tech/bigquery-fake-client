package jp.ne.opt.bigqueryfake

import org.scalatest.FunSpec

class FakeBigQuerySpec extends FunSpec {
  it("can be instantiated") {
    new FakeBigQuery(FakeBigQueryOptions.newBuilder.build())
  }
}
