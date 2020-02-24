package chap07

import util.SparkScalaUtil

import scala.collection.mutable.ListBuffer

object FindAssociationRulesScala {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: FindAssociationRules <input-path> <output-path>")
      sys.exit(1)
    }

    val sc = SparkScalaUtil.createLocalSparkContext("FindAssociationRulesScala")

    val input = args(0)
    val output = args(1)

    val transaction = sc.textFile(input)
    val patterns = transaction.flatMap(line => {
      val items = line.split(",").toList
      // 理解一下这行代码，flatMap还是原来的用法，这个地方多出了一个combinations，非常的强大
      //      (0 to items.size).flatMap(items.combinations).filter(xs => xs.nonEmpty)
      (0 to items.size) flatMap items.combinations filter (xs => xs.nonEmpty)
    }).map((_, 1))

    val combined = patterns.reduceByKey(_ + _)
    val subPatterns = combined.flatMap(pattern => {
      val result = ListBuffer.empty[(List[String], (List[String], Int))]
      result += ((pattern._1, (Nil, pattern._2)))

      val subList = for {
        i <- pattern._1.indices
        xs = pattern._1.take(i) ++ pattern._1.drop(i + 1)
        if xs.nonEmpty
      } yield (xs, (pattern._1, pattern._2))
      result ++= subList
      result.toList
    })
    val rules = subPatterns.groupByKey()
    val assocRules = rules.map(in => {
      val fromCount = in._2.find(p => p._1 == Nil).get
      val toList = in._2.filter(p => p._1 != Nil).toList
      if (toList.isEmpty) Nil
      else {
        val result = for {
          t2 <- toList
          confidence = t2._2.toDouble / fromCount._2.toDouble
          difference = t2._1 diff in._1
        } yield (in._1, difference, confidence)
        result
      }
    })
    val formatResult = assocRules.flatMap(f => {
      f.map(s => (s._1.mkString("[", ",", "]"), s._2.mkString("[", ",", "]"), s._3))
    })
    formatResult.saveAsTextFile(output)
  }
}
