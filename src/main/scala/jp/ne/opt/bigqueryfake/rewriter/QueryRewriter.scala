package jp.ne.opt.bigqueryfake.rewriter

import net.sf.jsqlparser.parser.CCJSqlParserUtil

class QueryRewriter(val statement: String) {
  def rewrite(): String = {
    val parsed = CCJSqlParserUtil.parse(statement)
    parsed.accept(new RewriteHandler)
    parsed.toString
  }
}
