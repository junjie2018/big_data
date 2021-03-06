package chap06

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MovingAverage {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage： MemoryMovingAverage <window> <number-of-partitions> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("MovingAverage")
    val sc = new SparkContext(sparkConf)

    val window = args(0).toInt
    val numPartitions = args(1).toInt
    val input = args(2)
    val output = args(3)

    val broadcastWindow = sc.broadcast(window)


    val rawData = sc.textFile(input)
    val valueToKey = rawData.map(line => {
      val tokens = line.split(",")
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val timestamp = dateFormat.parse(tokens(1)).getTime
      (CompositeKey(tokens(0), timestamp), TimeSeriesData(timestamp, tokens(2).toDouble))
    })
    val sortedData = valueToKey.repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(numPartitions))
    val keyValue = sortedData.map(k => (k._1.stockSymbol, k._2))
    val groupByStockSymbol = keyValue.groupByKey()
    val movingAverage = groupByStockSymbol.mapValues(values => {
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val queue = new mutable.Queue[Double]()
      for (timeSeriesData <- values) yield {
        queue.enqueue(timeSeriesData.closingStockPrice)
        if (queue.size > broadcastWindow.value) {
          queue.dequeue()
        }
        (dateFormat.format(new java.util.Date(timeSeriesData.timestamp)), queue.sum / queue.size)
      }
    })
    val formattedResult = movingAverage.flatMap(kv => {
      kv._2.map(v => kv._1 + ", " + v._1 + ", " + v._2.toString)
    })
    formattedResult.saveAsTextFile(output)
    sc.stop()
  }
}

case class CompositeKey(stockSymbol: String, timestamp: Long)

case class TimeSeriesData(timestamp: Long, closingStockPrice: Double)


object CompositeKey {
  implicit def ordering[A <: CompositeKey]: Ordering[A] = {
    Ordering.by(fk => (fk.stockSymbol, fk.timestamp))
  }
}

import org.apache.spark.Partitioner

class CompositeKeyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case k: CompositeKey => math.abs(k.stockSymbol.hashCode % numPartitions)
    case null => 0
    case _ => math.abs(key.hashCode() % numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: CompositeKeyPartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode(): Int = numPartitions
}