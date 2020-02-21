package chap06

import org.apache.spark.{SparkConf, SparkContext}

object MovingAverageInMemory {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovingAverageInMemory <window> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("MovingAverageInMemory")
    val sc = new SparkContext(sparkConf)

    val window = args(0).toInt
    val input = args(1)
    val output = args(2)

    val broadcaseWindow = sc.broadcast(window)

    val rawData = sc.textFile(input)
    val keyValue = rawData.map(line => {
      val tokens = line.split(",")
      (tokens(0), (tokens(1), tokens(2).toDouble))
    })
    val groupByStockSymbol = keyValue.groupByKey()
    val result = groupByStockSymbol.mapValues(values => {
      val dateFormat = new java.text.SimpleDateFormat("yyy-MM-dd")
      val sortedValues = values
        .map(s => (dateFormat.parse(s._1).getTime.toLong, s._2))
        .toSeq
        .sortBy(_._1)

      val queue = new scala.collection.mutable.Queue[Double]()
      // 我对这块的语法还是有点不太熟悉
      for (tup <- sortedValues) yield {
        queue.enqueue(tup._2)
        if (queue.size > broadcaseWindow.value)
          queue.dequeue()
        (dateFormat.format(new java.util.Date(tup._1)), queue.sum / queue.size)
      }
    })

    val formattedResult = result.flatMap(kv => {
      kv._2.map(v => kv._1 + ", " + v._1 + ", " + v._2.toString)
    })
    formattedResult.saveAsTextFile(output)

    sc.stop()
  }

}
