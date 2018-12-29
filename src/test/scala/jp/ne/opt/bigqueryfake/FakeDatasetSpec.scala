package jp.ne.opt.bigqueryfake

import java.sql.Connection

import com.google.cloud.bigquery.DatasetId
import org.scalatest.{MustMatchers, fixture}

class FakeDatasetSpec extends fixture.FunSpec with MustMatchers with DBFixture {
  def withFakeDataset(conn: Connection)(test: FakeDataset => Any) {
    val fakeBigQuery = new FakeBigQuery(conn)
    test(new FakeDataset(fakeBigQuery, DatasetId.of("foo")))
  }

  describe("create") {
    it("creates a dataset") { conn =>
      withFakeDataset(conn) { fakeDataset =>
        fakeDataset.create()
        fakeDataset.fakeBigQuery.queryHelper.list("SELECT schema_name FROM INFORMATION_SCHEMA.schemata") must
          contain("foo")
      }
    }
  }

  describe("get") {
    it("returns a dataset") { conn =>
      withFakeDataset(conn) { fakeDataset =>
        fakeDataset.fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.get.map(_.getDatasetId.getDataset) mustBe Some("foo")
      }
    }
  }

  describe("delete") {
    it("deletes a dataset") { conn =>
      withFakeDataset(conn) { fakeDataset =>
        fakeDataset.fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.delete()
        fakeDataset.fakeBigQuery.queryHelper.list("SELECT schema_name FROM INFORMATION_SCHEMA.schemata") mustNot
          contain("foo")
      }
    }
  }

  describe("update") {
    it("do nothing and returns a dataset") { conn =>
      withFakeDataset(conn) { fakeDataset =>
        fakeDataset.fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo;")
        fakeDataset.get.map(_.getDatasetId.getDataset) mustBe Some("foo")
      }
    }
  }

  describe("list") {
    it("returns all datasets") { conn =>
      val fakeBigQuery = new FakeBigQuery(conn)
      fakeBigQuery.queryHelper.execute("CREATE SCHEMA foo; CREATE SCHEMA bar;")
      FakeDataset.list(fakeBigQuery).map(_.getDatasetId.getDataset).toSet mustBe Set("foo", "bar")
    }
  }
}
