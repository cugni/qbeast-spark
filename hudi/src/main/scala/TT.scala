import org.apache.spark.sql.SparkSession
case class MyData(ts: Long, uuid: String)

object TT extends App {

  val spark = SparkSession.builder().appName("HudiTest").getOrCreate()

  val tableName = "trips_table"
  val basePath = s"file:////tmp/trips_table"

  val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
  import spark.implicits._
  val inserts = spark.range(1000000).map(x => MyData(x, x.toString))

  (inserts
    .coalesce(1)
    .write
    .format("hudi")
    .option("hoodie.clustering.inline", "true")
    .option("hoodie.clustering.inline.max.commits", "1")
    // .option("hoodie.clustering.plan.strategy.class", "")
    .option(
      "hoodie.clustering.execution.strategy.class",
      "io.qbeast.spark.hudi.QbeastHudiExecutionStrategy")
    .option("hoodie.table.name", tableName)
    .mode("overwrite")
    .save(basePath))

}
