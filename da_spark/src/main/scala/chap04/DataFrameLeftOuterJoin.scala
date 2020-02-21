package chap04

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameLeftOuterJoin {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: DataFrameLeftOuterJoin <users-data-path> <transactions-data-path> <output-path>")
      sys.exit(1)
    }

    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)

    val sparkConf = new SparkConf().setMaster("local").setAppName("DataFrameLeftOuterJoin")
    val spark = SparkSession.builder()
      .appName("DataFrameLeftOuterJoin")
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.types._

    val userSchema = StructType(Seq(
      StructField("userId", StringType, nullable = false),
      StructField("location", StringType, nullable = false)))
    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, nullable = false),
      StructField("productId", StringType, nullable = false),
      StructField("userId", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("price", DoubleType, nullable = false)))

    def userRows(line: String): Row = {
      val tokens = line.split(" ")
      Row(tokens(0), tokens(1))
    }

    def transactionRows(line: String): Row = {
      val tokens = line.split(" ")
      Row(tokens(0), tokens(1), tokens(2), tokens(3).toInt, tokens(4).toDouble)
    }

    val userRaw = sc.textFile(usersInputFile)
    val userRDDRows = userRaw.map(userRows)
    val users = spark.createDataFrame(userRDDRows, userSchema)

    val transactionRaw = sc.textFile(transactionsInputFile)
    val transactionRDDRows = transactionRaw.map(transactionRows)
    val transactions = spark.createDataFrame(transactionRDDRows, transactionSchema)

    // Approach1
    val joined = transactions.join(users, transactions("userId") === users("userId"))
    joined.printSchema()

    val product_location = joined.select(joined.col("productId"), joined.col("location"))
    val product_location_distinct = product_location.distinct()
    val products = product_location_distinct.groupBy("productId").count()

    products.show()
    products.write.save(output + "/approach1")
    products.rdd.saveAsTextFile(output + "/approach1_textFormat")

    // Approach2
    users.createTempView("users")
    transactions.createOrReplaceTempView("transactions")

    import spark.sql
    val sqlResult = sql(
      """SELECT productId, COUNT(DISTINCT location) AS locCount
        FROM transactions
        LEFT JOIN users ON transactions.userId = users.userId
        GROUP BY productId""")
    sqlResult.show()
    sqlResult.write.save(output + "/approach2")
    sqlResult.rdd.saveAsTextFile(output + "/approach2_textFormat")

    spark.stop()
  }
}
