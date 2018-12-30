package jp.ne.opt.bigqueryfake

import java.sql.Connection
import java.util.UUID

import com.google.cloud.bigquery._
import com.google.cloud.storage.BlobInfo
import org.scalatest.{MustMatchers, fixture}

class FakeLoadJobSpec extends fixture.FunSpec with MustMatchers with DBFixture {
  def withFakeTable(conn: Connection, tableDefinition: TableDefinition)(test: FakeTable => Any) {
    val fakeBigQuery = new FakeBigQuery(conn)
    fakeBigQuery.queryHelper.execute("CREATE SCHEMA IF NOT EXISTS bigqueryfake;")
    val fakeTable = new FakeTable(fakeBigQuery, TableId.of("bigqueryfake", "test"))
    fakeTable.create(tableDefinition)
    test(fakeTable)
  }

  describe("execute") {
    it("works with json") { conn =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING),
        Field.of("num", LegacySQLTypeName.INTEGER)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      val json =
        """
          |{"text": "aaa", "num": 1}
          |{"text": "bbb", "num": 2}
        """.stripMargin
      val bucket = "bigqueryfake"
      val path = s"temporary/${UUID.randomUUID()}"
      withFakeTable(conn, tableDefinition) { fakeTable =>
        fakeTable.fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.json()).build
        val fakeJob = new FakeLoadJob(fakeTable.fakeBigQuery, loadJobConfiguration).execute()
        fakeJob.getStatus.getState mustBe JobStatus.State.DONE
        fakeTable.fakeBigQuery.queryHelper.listValues(
          s"SELECT text, num FROM ${fakeTable.datasetName}.${fakeTable.tableName}"
        ).toSet mustBe Set(Seq("aaa", "1"), Seq("bbb", "2"))
      }
    }

    it("throws exception when format is not json") { conn =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      val json =
        """
          |{"text": "aaa"}
        """.stripMargin
      val bucket = "bigqueryfake"
      val path = s"temporary/${UUID.randomUUID()}"
      withFakeTable(conn, tableDefinition) { fakeTable =>
        fakeTable.fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.csv()).build
        an [UnsupportedOperationException] mustBe thrownBy { new FakeLoadJob(fakeTable.fakeBigQuery, loadJobConfiguration).execute() }
      }
    }

    it("throws exception with malformed json") { conn =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      val json =
        """
          |{"text":}
        """.stripMargin
      val bucket = "bigqueryfake"
      val path = s"temporary/${UUID.randomUUID()}"
      withFakeTable(conn, tableDefinition) { fakeTable =>
        fakeTable.fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.json()).build
        an [BigQueryException] mustBe thrownBy { new FakeLoadJob(fakeTable.fakeBigQuery, loadJobConfiguration).execute() }
      }
    }
  }
}
