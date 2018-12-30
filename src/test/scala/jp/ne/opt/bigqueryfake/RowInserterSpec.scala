package jp.ne.opt.bigqueryfake

import java.sql.Connection

import com.google.cloud.bigquery._
import org.scalatest.{MustMatchers, fixture}

class RowInserterSpec extends fixture.FunSpec with MustMatchers with DBFixture {
  def withFakeTable(conn: Connection, tableDefinition: TableDefinition)(test: FakeTable => Any) {
    val fakeBigQuery = new FakeBigQuery(conn)
    fakeBigQuery.queryHelper.execute("CREATE SCHEMA IF NOT EXISTS bigqueryfake;")
    val fakeTable = new FakeTable(fakeBigQuery, TableId.of("bigqueryfake", "test"))
    fakeTable.create(tableDefinition)
    test(fakeTable)
  }

  describe("insert") {
    it("works for non-partitioned table") { conn =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING),
        Field.of("num", LegacySQLTypeName.INTEGER)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      withFakeTable(conn, tableDefinition) { fakeTable =>
        val rowInserter = new RowInserter(fakeTable.fakeBigQuery, fakeTable.tableId)
        rowInserter.insert(Seq(Map("text" -> "aaa", "num" -> 1), Map("text" -> "bbb", "num" -> 2)))
        fakeTable.fakeBigQuery.queryHelper.listValues(
          s"SELECT text, num FROM ${fakeTable.datasetName}.${fakeTable.tableName}"
        ).toSet mustBe Set(Seq("aaa", "1"), Seq("bbb", "2"))
      }
    }

    it("works for partitioned table") { conn =>
      val schema = Schema.of(Field.of("name", LegacySQLTypeName.STRING))
      val tableDefinition = StandardTableDefinition.newBuilder
        .setSchema(schema).setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).build
      withFakeTable(conn, tableDefinition) { fakeTable =>
        val rowInserter = new RowInserter(fakeTable.fakeBigQuery, TableId.of(fakeTable.datasetName, s"${fakeTable.tableName}$$20181231"))
        rowInserter.insert(Seq(Map("name" -> "aaa"), Map("name" -> "bbb")))
        fakeTable.fakeBigQuery.queryHelper.listValues(
          s"SELECT name, _PARTITIONTIME FROM ${fakeTable.datasetName}.${fakeTable.tableName}"
        ).toSet mustBe Set(Seq("aaa", "2018-12-31 00:00:00.0"), Seq("bbb", "2018-12-31 00:00:00.0"))
      }
    }

    it("uses null partition when partition decorator is not given ") { conn =>
      val schema = Schema.of(Field.of("name", LegacySQLTypeName.STRING))
      val tableDefinition = StandardTableDefinition.newBuilder
        .setSchema(schema).setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).build
      withFakeTable(conn, tableDefinition) { fakeTable =>
        val rowInserter = new RowInserter(fakeTable.fakeBigQuery, TableId.of(fakeTable.datasetName, fakeTable.tableName))
        rowInserter.insert(Seq(Map("name" -> "aaa"), Map("name" -> "bbb")))
        fakeTable.fakeBigQuery.queryHelper.listValues(
          s"SELECT name, _PARTITIONTIME FROM ${fakeTable.datasetName}.${fakeTable.tableName}"
        ).toSet mustBe Set(Seq("aaa", null), Seq("bbb", null))
      }
    }

    it("throws an error when partition decorator is given for non-partitioned table") { conn =>
      val schema = Schema.of(Field.of("name", LegacySQLTypeName.STRING))
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      withFakeTable(conn, tableDefinition) { fakeTable =>
        val rowInserter = new RowInserter(fakeTable.fakeBigQuery, TableId.of(fakeTable.datasetName, s"${fakeTable.tableName}$$20181231"))
        an[BigQueryException] mustBe thrownBy {
          rowInserter.insert(Seq(Map("name" -> "aaa")))
        }
      }
    }
  }
}
