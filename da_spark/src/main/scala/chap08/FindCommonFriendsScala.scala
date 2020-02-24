package chap08

import util.SparkScalaUtil

object FindCommonFriendsScala {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: FindCommonFriends <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sc = SparkScalaUtil.createLocalSparkContext("FindCommonFriendsScala")

    val input = args(0)
    val output = args(1)

    val records = sc.textFile(input)
    val pairs = records.flatMap(s => {
      val tokens = s.split(",")
      val person = tokens(0).toLong
      val friends = tokens(1).split("\\s+").map(_.toLong).toList
      val result = for {
        i <- friends.indices
        friend = friends(i)
      } yield {
        if (person < friend)
          ((person, friend), friends)
        else
          ((friend, person), friends)
      }
      result
    })
    val grouped = pairs.groupByKey()
    val commonFriends = grouped.mapValues(iter => {
      val friendCount = for {
        list <- iter
        if list.nonEmpty
        friend <- list
      } yield (friend, 1)
      friendCount.groupBy(_._1).mapValues(_.unzip._2.sum).filter(_._2 > 1).map(_._1)
    })

    commonFriends.saveAsTextFile(output)

    sc.stop()
  }
}
