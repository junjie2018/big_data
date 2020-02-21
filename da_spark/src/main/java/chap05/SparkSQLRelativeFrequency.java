package chap05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSQLRelativeFrequency {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SparkSQLRelativeFrequency <neighbor-window> <input-dir> <output-dir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkSQLRelativeFrequency");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .appName("SparkSQLRelativeFrequency")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        int neighborWindow = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        Broadcast<Integer> broadcastWindow = sc.broadcast(neighborWindow);

        StructType rfSchema = new StructType(new StructField[]{
                new StructField("word", DataTypes.StringType, false, Metadata.empty()),
                new StructField("neighbour", DataTypes.StringType, false, Metadata.empty()),
                new StructField("frequency", DataTypes.IntegerType, false, Metadata.empty())
        });

        JavaRDD<String> rawData = sc.textFile(input);
        JavaRDD<Row> rowRDD = rawData.flatMap(line -> {
            List<Row> list = new ArrayList<>();
            String[] tokens = line.split("\\s+");
            for (int i = 0; i < tokens.length; i++) {
                int start = (i - broadcastWindow.value() < 0) ? 0 : i - broadcastWindow.value();
                int end = (i + broadcastWindow.value() >= tokens.length) ? tokens.length - 1 : i + broadcastWindow.value();

                for (int j = start; j <= end; j++) {
                    if (i == j) {
                        continue;
                    }

                    list.add(RowFactory.create(tokens[i], tokens[j], 1));
                }
            }
            return list.iterator();
        });

        Dataset<Row> rfDataset = spark.createDataFrame(rowRDD, rfSchema);

        rfDataset.createOrReplaceTempView("rfTable");
        String query = "SELECT a.word, a.neighbour, a.feq_total / b.total AS rf\n" +
                "FROM (\n" +
                "\tSELECT word, neighbour, SUM(frequency) AS feq_total\n" +
                "\tFROM rfTable\n" +
                "\tGROUP BY word, neighbour\n" +
                ") a\n" +
                "\tINNER JOIN (\n" +
                "\t\tSELECT word, SUM(frequency) AS total\n" +
                "\t\tFROM rfTable\n" +
                "\t\tGROUP BY word\n" +
                "\t) b\n" +
                "\tON a.word = b.word";
        Dataset<Row> sqlResult = spark.sql(query);
        sqlResult.show();
        sqlResult.write().parquet(output + "/parquetFormat");
        sqlResult.rdd().saveAsTextFile(output + "/textFormat");

        sc.close();
        spark.stop();
    }
}
