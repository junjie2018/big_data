package scala_version

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Driver {
  def main(args: Array[String]): Unit = {
    val inputFile = "C:\\Users\\Junjie\\Desktop\\spark2\\src\\main\\scala\\scala_version\\Driver.scala"
    val outputFile = "C:\\Users\\Junjie\\Desktop\\data\\tmp2"

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey({ case (x, y) => x + y })
    counts.saveAsTextFile(outputFile)
  }
}
