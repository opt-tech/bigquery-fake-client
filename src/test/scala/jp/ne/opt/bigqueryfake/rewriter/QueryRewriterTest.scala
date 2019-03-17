package jp.ne.opt.bigqueryfake.rewriter

import org.scalatest.{FunSpec, MustMatchers}

class QueryRewriterTest extends FunSpec with MustMatchers {
  describe("casting to datatype") {
    it("converts float64 to numeric") {
      val selectStatement = "select cast(1 as float64) from names"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select cast(1 as numeric) from names"
    }

    it("converts string to varchar") {
      val selectStatement = "select cast('a' as string) from names"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select cast('a' as varchar) from names"
    }
  }

  describe("functions") {
    it("converts nvl to coalesce") {
      val selectStatement = "select nvl(name, 'bob') from names"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select coalesce(name, 'bob') from names"
    }

    it("converts nvl2 to case statement") {
      val selectStatement = "select nvl2(name, 'a', 'b') from sales"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select case name is not null 'a' else 'b' end from sales"
    }

    it("removes timestamp call") {
      val selectStatement = "select timestamp(current_time()) from sales"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select (current_time()) from sales"
    }

    it("converts regexp_extract to substring") {
      val selectStatement = "select regexp_extract(name, '(\\w)') from sales"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select substring(name, '(\\w)') from sales"
    }
  }

  describe("FROM clause") {
    it("drops gcloud project_id when names are quoted as a whole") {
      val selectStatement = "select 1 from `project_id.dataset.table`"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select 1 from dataset.table"
    }

    it("drops gcloud project_id when names are individually quoted") {
      val selectStatement = "select 1 from `project_id`.`dataset`.`table`"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select 1 from dataset.table"
    }

    it("drops gcloud project_id when names are not quoted") {
      val selectStatement = "select 1 from project_id.dataset.table"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select 1 from dataset.table"
    }
  }

  describe("ORDER clause") {
    it("truns asc to asc nulls first") {
      val selectStatement = "select name from names order by name asc"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select name from names order by name asc nulls first"
      val selectStatementAscOmitted = "select name from names order by name"
      new QueryRewriter().rewrite(selectStatementAscOmitted).toLowerCase mustBe
        "select name from names order by name nulls first"
    }

    it("truns desc to desc nulls last") {
      val selectStatement = "select name from names order by name desc"
      new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
        "select name from names order by name desc nulls last"
    }
  }

  it("can parse sum function with case") {
    //val selectStatement = "select COALESCE(SUM(CASE WHEN TRUE THEN (CASE name WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN related_click_index + 2 THEN 1 ELSE 0 END) ELSE 0 END), 0) AS attribution__previous_last_time from names"
    val selectStatement = "select COALESCE(CASE name WHEN ('john ' + familyname) THEN 1 ELSE 0 END) from names"
    new QueryRewriter().rewrite(selectStatement).toLowerCase mustBe
      "select coalesce(case name when ('john ' + familyname) then 1 else 0 end) from names"
  }

  describe("with H2") {
    describe("functions") {
      it("converts date to trunc") {
        val selectStatement = "select date(current_time()) from names"
        new QueryRewriter(RewriteMode.H2).rewrite(selectStatement).toLowerCase mustBe
          "select trunc(current_time(), 'date') from names"
      }

      it("converts full outer join to union of left and right outer join") {
        val selectStatement = "select name from names full outer join addresses on names.address_id = address.id"
        new QueryRewriter(RewriteMode.H2).rewrite(selectStatement).toLowerCase mustBe
          "select name from names left join addresses on names.address_id = address.id union select name from names right join addresses on names.address_id = address.id"
      }
    }
  }
}

