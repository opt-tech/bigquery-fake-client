package jp.ne.opt.bigqueryfake

import java.util.UUID

import com.google.cloud.bigquery._
import com.google.cloud.storage.BlobInfo
import org.scalatest.{MustMatchers, fixture}

class FakeLoadJobSpec extends fixture.FunSpec with MustMatchers with ServiceFixture {
  def withFakeTable(fakeBigQuery: FakeBigQuery, tableDefinition: TableDefinition)(test: FakeTable => Any) {
    fakeBigQuery.queryHelper.execute("CREATE SCHEMA IF NOT EXISTS bigqueryfake;")
    val fakeTable = new FakeTable(fakeBigQuery, TableId.of("bigqueryfake", "test"))
    fakeTable.create(tableDefinition)
    test(fakeTable)
  }

  describe("create") {
    it("works with json") { fakeBigQuery =>
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
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.json()).build
        val fakeJob = new FakeLoadJob(fakeBigQuery, loadJobConfiguration).create()
        fakeJob.getStatus.getState mustBe JobStatus.State.DONE
        fakeBigQuery.queryHelper.listValues(
          s"SELECT text, num FROM ${fakeTable.datasetName}.${fakeTable.tableName}"
        ).toSet mustBe Set(Seq("aaa", "1"), Seq("bbb", "2"))
      }
    }

    it("throws exception when format is not json") { fakeBigQuery =>
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
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.csv()).build
        an [UnsupportedOperationException] mustBe thrownBy { new FakeLoadJob(fakeBigQuery, loadJobConfiguration).create() }
      }
    }

    it("throws exception with malformed json") { fakeBigQuery =>
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
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.storage.create(BlobInfo.newBuilder(bucket, path).build(), json.getBytes("UTF-8"))
        val loadJobConfiguration = LoadJobConfiguration.newBuilder(fakeTable.tableId, s"gs://${bucket}/${path}", FormatOptions.json()).build
        an [BigQueryException] mustBe thrownBy { new FakeLoadJob(fakeBigQuery, loadJobConfiguration).create() }
      }
    }
  }
}
