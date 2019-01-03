package jp.ne.opt.bigqueryfake

import java.sql.{Connection, DriverManager}

import com.google.cloud.storage.Storage

import scala.beans.BeanProperty

class FakeBigQueryOptions(builder: FakeBigQueryOptions.Builder) {
  val connection: Connection = Option(builder.connection).getOrElse {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection("jdbc:h2:mem:;MODE=PostgreSQL;DATABASE_TO_UPPER=false")
  }
  val storage: Storage = Option(builder.storage).getOrElse(new FakeStorage)
  val projectId: String = builder.projectId
}

object FakeBigQueryOptions {
  class Builder {
    @BeanProperty
    var connection: Connection = _
    @BeanProperty
    var storage: Storage = _
    @BeanProperty
    var projectId: String = "bigqueryfake"
    def build(): FakeBigQueryOptions = new FakeBigQueryOptions(this)
  }

  def getDefaultInstance: FakeBigQueryOptions = newBuilder.build()
  def newBuilder: Builder = new Builder
}