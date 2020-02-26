import org.apache.spark.sql.SparkSession

object SimpleApp extends App{
    val spark = SparkSession.builder().master("local").getOrCreate()
    val df = spark.read.json("../data/people.json")
    df.createOrReplaceTempView("people")
    val sql = spark.sql("select * from people")
    sql.show()

    sql.repartition(1).write.mode("overwrite").option("header", "true").parquet("../data")
    println("job done")
}