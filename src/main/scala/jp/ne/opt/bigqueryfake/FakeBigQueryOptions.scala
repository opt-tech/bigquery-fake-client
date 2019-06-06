package jp.ne.opt.bigqueryfake

import java.sql.{Connection, DriverManager}

import com.google.cloud.storage.Storage
import jp.ne.opt.bigqueryfake.rewriter.RewriteMode

class FakeBigQueryOptions(builder: FakeBigQueryOptions.Builder) {
  val connection: Connection = Option(builder.connection).getOrElse {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection("jdbc:h2:mem:;MODE=PostgreSQL;DATABASE_TO_UPPER=false")
  }
  val rewriteMode: RewriteMode = Option(builder.rewriteMode).getOrElse(
    Option(builder.connection).map(_ => RewriteMode.PostgreSQL).getOrElse(RewriteMode.H2)
  )
  val storage: Storage = Option(builder.storage).getOrElse(new FakeStorage)
  val projectId: String = builder.projectId

  def getService: FakeBigQuery = new FakeBigQuery(this)
}

object FakeBigQueryOptions {
  class Builder {
    private[bigqueryfake] var connection: Connection = _
    private[bigqueryfake] var rewriteMode: RewriteMode = _
    private[bigqueryfake] var storage: Storage = _
    private[bigqueryfake] var projectId: String = "bigqueryfake"

    def setConnection(connection: Connection): Builder = {
      this.connection = connection
      this
    }

    def setRewriteMode(rewriteMode: RewriteMode): Builder = {
      this.rewriteMode = rewriteMode
      this
    }

    def setRewriteMode(rewriteMode: String): Builder = {
      this.rewriteMode = RewriteMode.valueOf(rewriteMode)
      this
    }

    def setStorage(storage: Storage): Builder = {
      this.storage = storage
      this
    }

    def setProjectId(projectId: String): Builder = {
      this.projectId = projectId
      this
    }

    def build(): FakeBigQueryOptions = new FakeBigQueryOptions(this)
  }

  def getDefaultInstance: FakeBigQueryOptions = newBuilder.build()
  def newBuilder: Builder = new Builder
}