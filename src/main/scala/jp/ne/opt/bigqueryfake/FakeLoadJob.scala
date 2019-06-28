package jp.ne.opt.bigqueryfake

import java.net.URI
import java.nio.charset.StandardCharsets

import com.google.cloud.bigquery.JobInfo.CreateDisposition
import com.google.cloud.bigquery._
import org.typelevel.jawn.ast._
import org.typelevel.jawn.{Facade, Parser}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class FakeLoadJob(fakeBigQuery: FakeBigQuery, config: LoadJobConfiguration) {
  val fakeTable = FakeTable(fakeBigQuery, config.getDestinationTable)

  def create(): Job = {
    if (config.getFormat != FormatOptions.json().getType)
      throw new UnsupportedOperationException(s"Unsupported load format: ${config.getFormat}")

    fakeTable.get.getOrElse {
      config.getCreateDisposition match {
        case CreateDisposition.CREATE_IF_NEEDED =>
          val definition = StandardTableDefinition.newBuilder().
            setSchema(config.getSchema).build()
          fakeTable.create(definition)
        case _ => throw new BigQueryException(404, s"Table ${config.getDestinationTable.getTable} does not exist")
      }
    }

    val rows = downloadJsons().filterNot { json => json.isEmpty || json.matches("^\\s+$") }.map { json =>
      implicit val facade: Facade[_] = JawnFacade
      Parser.parseFromString(json) match {
        case Success(JObject(vs)) =>
          vs.map { case (key, value) =>
              key -> (value match {
                case JString(s) => s
                case v :JValue => v.render()
              })
          }.toMap
        case Failure(e) => throw new BigQueryException(400, s"Failed to parse json: $json", e)
        case _ => throw new BigQueryException(400, s"Failed to parse json: $json")
      }
    }
    new RowInserter(fakeBigQuery, fakeTable.tableId).insert(rows)
    FakeBuilder.newFakeJob(this.fakeBigQuery, JobInfo.of(config))
  }

  private def downloadJsons(): Seq[String] =
    config.getSourceUris.asScala.flatMap { uriString =>
      val uri = new URI(uriString)
      new String(
        fakeBigQuery.storage.readAllBytes(uri.getHost, uri.getPath.replaceFirst("^/", "")),
        StandardCharsets.UTF_8
      ).split("[\r\n]+")
    }.toSeq
}

