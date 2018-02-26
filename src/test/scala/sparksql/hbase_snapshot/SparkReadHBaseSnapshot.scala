package sparksql.hbase_snapshot

import org.apache.spark.sql.SparkSession

object SparkReadHBaseSnapshot {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkReadHBaseSnapshot")
      .getOrCreate()

    val hbasetable = spark.read.format("sparksql.hbase_snapshot").options(Map(
      "sparksql.table.schema" -> "(key string, title string, url string)",
      "hbase.zookeeper.quorum" -> "localhost",
      "hbase.table.name" -> "crawled",
      "hbase.table.schema" -> "(:key , data:title , basic:url)",
      "hbase.snapshot.path" -> "/user/sms/hbase_snapshots",
      "hbase.snapshot.name" -> "crawled_snapshot"
    )).load()

    hbasetable.printSchema()
    hbasetable.show()

    spark.stop()

  }
}
