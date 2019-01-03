package jp.ne.opt.bigqueryfake

import com.google.cloud.bigquery.DatasetId
import org.scalatest.{MustMatchers, fixture}

class FakeDatasetSpec extends fixture.FunSpec with MustMatchers with ServiceFixture {
  def withFakeDataset(fakeBigQuery: FakeBigQuery)(test: FakeDataset => Any) {
    test(new FakeDataset(fakeBigQuery, DatasetId.of("foo")))
  }

  describe("create") {
    it("creates a dataset") { fakeBigQuery =>
      withFakeDataset(fakeBigQuery) { fakeDataset =>
        fakeDataset.create()
        fakeBigQuery.queryHelper.list("SELECT schema_name FROM INFORMATION_SCHEMA.schemata") must
          contain("foo")
      }
    }
  }

  describe("get") {
    it("returns a dataset") { fakeBigQuery =>
      withFakeDataset(fakeBigQuery) { fakeDataset =>
        fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.get.map(_.getDatasetId.getDataset) mustBe Some("foo")
      }
    }
  }

  describe("delete") {
    it("deletes a dataset") { fakeBigQuery =>
      withFakeDataset(fakeBigQuery) { fakeDataset =>
        fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.delete()
        fakeBigQuery.queryHelper.list("SELECT schema_name FROM INFORMATION_SCHEMA.schemata") mustNot
          contain("foo")
      }
    }
  }

  describe("update") {
    it("do nothing and returns a dataset") { fakeBigQuery =>
      withFakeDataset(fakeBigQuery) { fakeDataset =>
        fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.get.map(_.getDatasetId.getDataset) mustBe Some("foo")
      }
    }
  }

  describe("list") {
    it("returns all datasets") { fakeBigQuery =>
      fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo; CREATE SCHEMA bar;")
      FakeDataset.list(fakeBigQuery).map(_.getDatasetId.getDataset).toSet mustBe Set("foo", "bar")
    }
  }
}
