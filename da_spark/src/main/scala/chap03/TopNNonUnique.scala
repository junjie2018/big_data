package chap03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.SortedMap

object TopNNonUnique {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: TopNNonUnique <input>")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val N = sc.broadcast(2)
    val path = args(0)

    val input = sc.textFile(path)
    val kv = input.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1).toInt)
    })

    val uniqueKeys = kv.reduceByKey(_ + _)
    val partitions = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach(tuple => {
        sortedMap += tuple.swap
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      })
      sortedMap.takeRight(N.value).toIterator
    })

    val allTop10 = partitions.collect()
    val finalTop10 = SortedMap.empty[Int, String].++:(allTop10)
    val resultUsingMapPartition = finalTop10.takeRight(N.value)

    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(",")}")
    }

//    val moreConciseApproach = kv.combineByKey((v: Int) => v, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
//      .map(_.swap)
//      .groupByKey()
//      .sortByKey(ascending = false)
//      .take(N.value)
//    moreConciseApproach.foreach {
//      case (k, v) => println(s"$k \t ${v.mkString(",")}")
//    }
    sc.stop()
  }
}
