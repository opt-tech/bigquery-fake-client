package jp.ne.opt.bigqueryfake

import com.google.cloud.bigquery._
import org.scalatest.{MustMatchers, fixture}

import scala.collection.JavaConverters._

class FakeTableSpec extends fixture.FunSpec with MustMatchers with ServiceFixture {
  def withFakeTable(fakeBigQuery: FakeBigQuery)(test: FakeTable => Any) {
    fakeBigQuery.queryHelper.execute("CREATE SCHEMA IF NOT EXISTS bigqueryfake;")
    test(new FakeTable(fakeBigQuery, TableId.of("bigqueryfake", "test")))
  }

  describe("create") {
    it("creates a non-partitioned table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        val schema = Schema.of(Field.of("text", LegacySQLTypeName.STRING))
        val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
        fakeTable.create(tableDefinition).getTableId.getTable mustBe "test"
        fakeBigQuery.queryHelper.
          list("SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_schema = 'bigqueryfake' and table_name = 'test';").toSet mustBe Set("text")
      }
    }

    it("creates a partitioned table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        val schema = Schema.of(Field.of("text", LegacySQLTypeName.STRING))
        val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).
          setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).build
        fakeTable.create(tableDefinition).getTableId.getTable mustBe "test"
        fakeBigQuery.queryHelper
          .list("SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_schema = 'bigqueryfake' and table_name = 'test';").map(_.toLowerCase).toSet mustBe Set("text", "_partitiontime")
      }
    }
  }

  describe("get") {
    it("returns a non-partitioned table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.test (name VARCHAR(65536));")
        val tableDefinition = fakeTable.get.get.getDefinition[StandardTableDefinition]
        tableDefinition.getSchema.getFields.iterator().asScala.map(_.getName).toSet mustBe Set("name")
        tableDefinition.getTimePartitioning mustBe null
      }
    }

    it("returns a partitioned table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.test (name VARCHAR(65536), _PARTITIONTIME timestamp);")
        val tableDefinition = fakeTable.get.get.getDefinition[StandardTableDefinition]
        tableDefinition.getSchema.getFields.iterator().asScala.map(_.getName).toSet mustBe Set("name")
        tableDefinition.getTimePartitioning mustBe TimePartitioning.of(TimePartitioning.Type.DAY)
      }
    }
  }

  describe("delete") {
    it("deletes a table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.test (name VARCHAR(65536), _PARTITIONTIME timestamp);")
        fakeTable.delete()
        fakeBigQuery.queryHelper.
          list("SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema = 'bigqueryfake';").toSet mustNot
          contain("test")
      }
    }
  }

  describe("update") {
    it("do nothing and returns a table") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.test (name VARCHAR(65536), _PARTITIONTIME timestamp);")
        fakeTable.update().getTableId.getTable mustBe "test"
      }
    }
  }

  describe("list") {
    it("returns all tables") { fakeBigQuery =>
      withFakeTable(fakeBigQuery) { fakeTable =>
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.nonpartitioned (name VARCHAR(65536));")
        fakeBigQuery.queryHelper.execute("CREATE TABLE bigqueryfake.partitioned (name VARCHAR(65536), _PARTITIONTIME timestamp);")
        FakeTable.list(fakeBigQuery, DatasetId.of("bigqueryfake")).map(_.getTableId.getTable).toSet mustBe Set("nonpartitioned", "partitioned")
      }
    }
  }
}
