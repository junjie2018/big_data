package chap03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.SortedMap

object TopN {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: TopN <input>")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    /*
      我查看了broadcast的用法，感觉在这个地方的用法并不正确，Integer的成本很小，完全可以多copy到
      各个结点。
     */
    val N = sc.broadcast(10)
    val path = args(0)

    val input = sc.textFile(path)
    val pair = input.map(line => {
      val tokens = line.split(",")
      (tokens(1).toInt, tokens)
    })

    // todo 关注这个代码的作用
//    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, Array[String]]
      itr.foreach { tuple => {
        sortedMap += tuple
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    /*
        理解这段代码：
            1.将所有分区的结果收集起来，形成一个包含所有分区计算结果的集合
            2.创建一个新的SortedMap，将收集到的所有结果，传递进去
            3.取这个新的SortedMap的前n个值，便是需求的结果
     */
    val allTopN = partitions.collect()
    val finalTopN = SortedMap.empty[Int, Array[String]].++:(allTopN)
    val resultUsingMapPartition = finalTopN.takeRight(N.value)

    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }

//    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)
//    moreConciseApproach.foreach {
//      case (k, v) => println(s"$k \t ${v.flatten.mkString(",")}")
//    }
//
    sc.stop()
  }
}
