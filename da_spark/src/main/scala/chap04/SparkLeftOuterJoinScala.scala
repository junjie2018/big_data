package chap04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object SparkLeftOuterJoinScala {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SparkLeftOuterJoin <users> <transactions> <output>")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("SparkLeftOuterJoinScala")
    val sc = new SparkContext(conf)

    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)

    val usersRaw = sc.textFile(usersInputFile)
    val users = usersRaw.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1))
    })

    val transactionRaw = sc.textFile(transactionsInputFile)
    val transactions = transactionRaw.map(line => {
      val tokens = line.split(" ")
      (tokens(2), tokens(1))
    })

    val joined = transactions leftOuterJoin users
    val productLocations = joined.values.map(f => (f._1, f._2.getOrElse("unknown")))
    val productByLocations = productLocations.groupByKey()
    val productWithUniqueLocations = productByLocations.mapValues(_.toSet)
    val result = productWithUniqueLocations.map(t => (t._1, t._2.size))
    result.saveAsTextFile(output)
  }
}
