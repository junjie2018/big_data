package chap05

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object RelativeFrequencyScala {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: RelativeFrequency <neighbour-window> <input-dir> <output-dir>")
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
    val pairs = rawData.flatMap(line => {
      val tokens = line.split("\\s+")
      for {
        i <- 0 until tokens.length
        start = if (i - broadcastWindow.value < 0) 0 else i - broadcastWindow.value
        end = if (i + broadcastWindow.value >= tokens.length) tokens.length - 1 else i + broadcastWindow.value
        j <- start to end if j != i
      } yield (tokens(i), (tokens(j), 1))
    })

    val totalByKey = pairs.map(t => (t._1, t._2._2)).reduceByKey(_ + _)
    val grouped = pairs.groupByKey()
    // 这块不是很理解
    val uniquePairs = grouped.flatMapValues(_.groupBy(_._1).mapValues(_.unzip._2.sum))
    val joined = uniquePairs join totalByKey

    val relativeFrequency = joined.map(t => {
      ((t._1, t._2._1._1), t._2._1._2.toDouble / t._2._2.toDouble)
    })
    val formatResult_tab_separated = relativeFrequency.map(t => t._1._1 + "\t" + t._1._2 + "\t" + t._2)
    formatResult_tab_separated.saveAsTextFile(output)

    sc.stop()
  }

}
