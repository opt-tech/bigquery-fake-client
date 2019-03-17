# bigquery-fake-client

[![Build Status](https://travis-ci.org/opt-tech/bigquery-fake-client.svg?branch=master)](https://travis-ci.org/opt-tech/bigquery-fake-client)

bigquery-fake-client imitates Google's BigQuery with help of an RDB backends(H2, PostgreSQL), useful for testing Scala/Java applications locally or in CI. It has identical APIs with [Google Cloud Java Client for BigQuery](https://github.com/googleapis/google-cloud-java/tree/master/google-cloud-clients/google-cloud-bigquery) so switching between fake and real one is simple enough.

## Installation

Check out git repository and build locally until upcoming Maven release:

```bash
$ sbt +publishLocal
```

## Usage

Using default backend of H2:

```scala
import jp.ne.opt.bigqueryfake._
import com.google.cloud.bigquery._
import scala.collection.JavaConverters._

val bigQuery = FakeBigQueryOptions.getDefaultInstance.getService
bigQuery.create(DatasetInfo.of("dataset"))
val schema = Schema.of(Field.of("text", LegacySQLTypeName.STRING))
val tableDefinition = StandardTableDefinition.newBuilder.setSchema(schema).build
bigQuery.create(TableInfo.of(TableId.of("dataset", "table"), tableDefinition))

bigQuery.insertAll(InsertAllRequest.of("dataset", "table", Seq(InsertAllRequest.RowToInsert.of("dummy_id", Map("text" -> "aaa").asJava)).asJava))
val result = bigQuery.query(QueryJobConfiguration.of("select * from dataset.table"))
result.getValues.asScala // => Wrappers([FieldValue{attribute=PRIMITIVE, value=aaa}])
```

To use PostgreSQL, pass in externally created PostgreSQL connection:

```scala
import jp.ne.opt.bigqueryfake._

Class.forName("org.postgresql.Driver")
val conn = java.sql.DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/bigqueryfake?user=postgres")
val bigQuery = FakeBigQueryOptions.newBuilder.setConnection(conn).build().getService
```

## License

Apache 2.0
