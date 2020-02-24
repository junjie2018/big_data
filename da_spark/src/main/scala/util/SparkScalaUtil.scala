package util

import org.apache.spark.{SparkConf, SparkContext}

object SparkScalaUtil {
  def createLocalSparkContext(applicationName: String): SparkContext = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(applicationName)
    new SparkContext(sparkConf)
  }
}
