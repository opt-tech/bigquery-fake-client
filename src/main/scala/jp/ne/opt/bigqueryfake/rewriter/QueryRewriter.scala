package jp.ne.opt.bigqueryfake.rewriter

import net.sf.jsqlparser.parser.CCJSqlParserUtil

class QueryRewriter(val mode: RewriteMode = RewriteMode.H2) {
  def rewrite(statement: String): String = {
    val parsed = CCJSqlParserUtil.parse(statement)
    parsed.accept(mode.handler)
    parsed.toString
  }
}