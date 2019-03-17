package jp.ne.opt.bigqueryfake.rewriter

import net.sf.jsqlparser.expression.{Function, StringValue}
import net.sf.jsqlparser.expression.operators.relational.ExpressionList
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.{Join, PlainSelect, Select}

import scala.collection.JavaConverters._

class H2RewriteHandler extends RewriteHandler {
  override def visit(function: Function): Unit = {
    function.getName.toLowerCase match {
      case "date" =>
        function.setName("TRUNC")
        val parameters = function.getParameters.getExpressions.asScala
        parameters.append(new StringValue("'date'"))
        function.setParameters(new ExpressionList(parameters.asJava))
      case _ =>
    }
    super.visit(function)
  }

  override def visit(select: Select): Unit = {
    select.getSelectBody match {
      case plainSelect: PlainSelect =>
        def mkSelect: PlainSelect = {
          val select = new PlainSelect
          select.setSelectItems(plainSelect.getSelectItems)
          select.setFromItem(plainSelect.getFromItem)
          select
        }

        Option(plainSelect.getJoins).map(_.asScala).getOrElse(Nil).collect {
          case join if join.isFull =>
            val left = new Join
            left.setLeft(true)
            left.setRightItem(join.getRightItem)
            left.setOnExpression(join.getOnExpression)
            val expr1 = mkSelect
            expr1.setJoins(Seq(left).asJava)

            val right = new Join
            right.setRight(true)
            right.setRightItem(join.getRightItem)
            right.setOnExpression(join.getOnExpression)
            val expr2 = mkSelect
            expr2.setJoins(Seq(right).asJava)

            val statement = CCJSqlParserUtil.parse(s"$expr1 UNION $expr2")
            select.setSelectBody(statement.asInstanceOf[Select].getSelectBody)
          case _ =>
        }
      case _ =>
    }
    super.visit(select)
  }
}