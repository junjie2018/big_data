package chap01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtil;

public class SecondarySortUsingRepartitionAndSortWithPartitions {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: <number-of-partitions> <input-dir> <output-dir>");
            System.exit(1);
        }

        int partitions = Integer.parseInt(args[0]);
        String inputPath = args[1], outputPath = args[2];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("SecondarySortUsingRepartitionAndSortWithPartitions");

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // 转换为：JavaPairRDD<Tuple2<String, Integer>, Integer>
        JavaPairRDD<Tuple2<String, Integer>, Integer> valueToKey = lines.mapToPair(t -> {
            String[] array = t.split(",");
            Integer value = Integer.parseInt(array[2]);
            Tuple2<String, Integer> key = new Tuple2<>(array[0] + "-" + array[1], value);
            return new Tuple2<>(key, value);
        });

        JavaPairRDD<Tuple2<String, Integer>, Integer> sorted = valueToKey.repartitionAndSortWithinPartitions(
                new CustomPartitionerScala(partitions),
                // 这个地方直接用lambda会报无法序列化的异常
                TupleComparatorDescending.INSTANCE);


        JavaPairRDD<String, Integer> result = sorted.mapToPair(t -> new Tuple2<>(t._1._1, t._2));

        result.saveAsTextFile(outputPath);

        ctx.close();
    }
}
