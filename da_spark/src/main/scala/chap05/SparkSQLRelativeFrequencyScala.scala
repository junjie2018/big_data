package chap05

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object SparkSQLRelativeFrequencyScala {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SparkSQLRelativeFrequency <neighbor-window> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("LeftOuterJoin")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val neighborWindow = args(0).toInt
    val input = args(1)
    val output = args(2)

    val broadcastWindow = sc.broadcast(neighborWindow)

    val rawData = sc.textFile(input)
    val rfSchema = StructType(Seq(
      StructField("word", StringType, nullable = false),
      StructField("neighbour", StringType, nullable = false),
      StructField("frequency", IntegerType, nullable = false)
    ))

    val rowRDD = rawData.flatMap(line => {
      val tokens = line.split("\\s+")

      for {
        i <- 0 until tokens.length
        start = if (i - broadcastWindow.value < 0) 0 else i - broadcastWindow.value
        end = if (i + broadcastWindow.value >= tokens.length) tokens.length - 1 else i + broadcastWindow.value
        j <- start to end if j != i
      } yield Row(tokens(i), tokens(j), 1)

    })

    val rfDataFrame = spark.createDataFrame(rowRDD, rfSchema)
    rfDataFrame.createOrReplaceTempView("rfTable")

    val query =
      """SELECT a.word, a.neighbour, a.feq_total / b.total AS rf
                  FROM (
                  	SELECT word, neighbour, SUM(frequency) AS feq_total
                  	FROM rfTable
                  	GROUP BY word, neighbour
                  ) a
                  	INNER JOIN (
                  		SELECT word, SUM(frequency) AS total
                  		FROM rfTable
                  		GROUP BY word
                  	) b
                  	ON a.word = b.word"""

    import spark.sql
    val sqlResult = sql(query)
    sqlResult.show()
    sqlResult.write.save(output + "/parquetFormat")
    sqlResult.rdd.saveAsTextFile(output + "/textFormat")
    spark.stop()
  }
}
