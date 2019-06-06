package jp.ne.opt.bigqueryfake.rewriter


sealed trait RewriteMode {
  def matches(str: String): Boolean
  def handler: RewriteHandler
}

object RewriteMode {
  object H2 extends RewriteMode {
    def matches(str: String): Boolean = str.toLowerCase.matches("h2")
    val handler: RewriteHandler = new H2RewriteHandler
  }
  object PostgreSQL extends RewriteMode {
    def matches(str: String): Boolean = str.toLowerCase.matches("postgres(ql)?")
    val handler: RewriteHandler = new PostgreSQLRewriteHandler
  }
  val values: Seq[RewriteMode] = Seq(H2, PostgreSQL)
  def valueOf(str: String): RewriteMode = values.find(_.matches(str)).getOrElse(
    throw new IllegalArgumentException(s"Unknown rewrite mode: ${str}")
  )
}