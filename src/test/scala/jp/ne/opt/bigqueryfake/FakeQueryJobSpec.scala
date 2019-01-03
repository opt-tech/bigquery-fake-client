package jp.ne.opt.bigqueryfake

import java.time.{LocalDateTime, ZoneOffset}

import com.google.cloud.bigquery._
import org.scalatest.{MustMatchers, fixture}

import scala.collection.JavaConverters._

class FakeQueryJobSpec extends fixture.FunSpec with MustMatchers with ServiceFixture {
  def withFakeTable(fakeBigQuery: FakeBigQuery, tableDefinition: TableDefinition)(test: FakeTable => Any) {
    fakeBigQuery.queryHelper.execute("CREATE SCHEMA IF NOT EXISTS bigqueryfake;")
    val fakeTable = new FakeTable(fakeBigQuery, TableId.of("bigqueryfake", "test"))
    fakeTable.create(tableDefinition)
    test(fakeTable)
  }

  describe("getResult") {
    it("works with non-partitioned table") { fakeBigQuery =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING),
        Field.of("num", LegacySQLTypeName.INTEGER)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.queryHelper.execute(
          "INSERT INTO bigqueryfake.test (text, num) VALUES ('aaa', 1), ('bbb', 2);"
        )
        val queryJobConfiguration = QueryJobConfiguration.newBuilder("SELECT * FROM bigqueryfake.test").build
        val result = new FakeQueryJob(fakeBigQuery, queryJobConfiguration).fetchResult()
        result.getValues.asScala.map { row =>
          0.until(row.size()).map(i => row.get(i).getValue)
        }.toSet mustBe Set(Seq("aaa", 1), Seq("bbb", 2))
      }
    }

    it("works with partitioned table") { fakeBigQuery =>
      val schema = Schema.of(
        Field.of("text", LegacySQLTypeName.STRING)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema)
        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).build
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.queryHelper.execute(
          "INSERT INTO bigqueryfake.test (text, _PARTITIONTIME) VALUES ('aaa', '2018-12-30 00:00:00'), ('bbb', '2018-12-31 00:00:00');"
        )
        val queryJobConfiguration = QueryJobConfiguration.newBuilder("SELECT * FROM bigqueryfake.test").build
        val result = new FakeQueryJob(fakeBigQuery, queryJobConfiguration).fetchResult()
        val dateTime1 = LocalDateTime.of(2018, 12, 30, 0, 0).atZone(ZoneOffset.UTC).toInstant.toEpochMilli * 1000
        val dateTime2 = LocalDateTime.of(2018, 12, 31, 0, 0).atZone(ZoneOffset.UTC).toInstant.toEpochMilli * 1000
        result.getValues.asScala.map { row =>
          Seq(row.get(0).getStringValue, row.get(1).getTimestampValue)
        }.toSet mustBe Set(Seq("aaa", dateTime1), Seq("bbb", dateTime2))
      }
    }

    it("works with all field types") { fakeBigQuery =>
      val schema = Schema.of(
        Field.of("byte", LegacySQLTypeName.BYTES),
        Field.of("string", LegacySQLTypeName.STRING),
        Field.of("integer", LegacySQLTypeName.INTEGER),
        Field.of("float", LegacySQLTypeName.FLOAT),
        Field.of("numeric", LegacySQLTypeName.NUMERIC),
        Field.of("boolean", LegacySQLTypeName.BOOLEAN),
        Field.of("timestamp", LegacySQLTypeName.TIMESTAMP),
        Field.of("date", LegacySQLTypeName.DATE),
        Field.of("time", LegacySQLTypeName.TIME),
        Field.of("datetime", LegacySQLTypeName.DATETIME)
      )
      val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
      withFakeTable(fakeBigQuery, tableDefinition) { fakeTable =>
        fakeBigQuery.queryHelper.execute(
          s"""
             |INSERT INTO bigqueryfake.test (byte, string, integer, float, numeric, boolean, timestamp, date, time, datetime)
             | VALUES (${fakeBigQuery.queryHelper.useCompatibleOneOf("'4142'", "E'\\\\x4142'")}, 'text', 1, 2.0, 3, TRUE, '2019-01-01 19:01:01', '2019-01-02', '20:19:01', '2018-12-31 23:59:59');
           """.stripMargin
        )
        val queryJobConfiguration = QueryJobConfiguration.newBuilder("SELECT * FROM bigqueryfake.test").build
        val result = new FakeQueryJob(fakeBigQuery, queryJobConfiguration).fetchResult()
        result.getValues.asScala.map { row =>
          0.until(row.size()).map(i => row.get(i).getValue)
        }.toSeq(0) mustBe Seq("QUI=", "text", 1, 2.0, new java.math.BigDecimal(3), true, "1546369261.000000", "2019-01-02", "20:19:01", "1546300799.000000")
      }
    }
  }
}
