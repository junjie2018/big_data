package chap04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LeftOuterJoin {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: LeftOuterJoin <users-data-path> <transactions-data-path> <output-path>")
      sys.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("LeftOuterJoin")
    val sc = new SparkContext(conf)

    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)

    val usersRaw = sc.textFile(usersInputFile)
    val users = usersRaw.map(line => {
      val tokens = line.split(" ")
      (tokens(0), ("L", tokens(1)))
    })

    val transactionRaw = sc.textFile(transactionsInputFile)
    val transactions = transactionRaw.map(line => {
      val tokens = line.split(" ")
      (tokens(2), ("P", tokens(1)))
    })

    val all = users union transactions
    val grouped = all.groupByKey()
    val productLocations = grouped.flatMap({
      case (userId, iterable) =>
        val (location, products) = iterable span (_._1 == "L")
        val loc = location.headOption.getOrElse("L", "UNKNOWN")
        products.filter(_._1 == "P").map(p => (p._2, loc._2)).toSet
    })
    val productByLocations = productLocations.groupByKey()

    val result = productByLocations.map(t=>(t._1,t._2.size))

    result.saveAsTextFile(output)

    sc.stop()
  }
}
