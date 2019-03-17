package jp.ne.opt.bigqueryfake.rewriter


sealed trait RewriteMode {
  def handler: RewriteHandler
}

object RewriteMode {
  object H2 extends RewriteMode {
    val handler: RewriteHandler = new H2RewriteHandler
  }
  object PostgreSQL extends RewriteMode {
    val handler: RewriteHandler = new PostgreSQLRewriteHandler
  }
}
