package chapter01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SecondarySortUsingCombineKey {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
        }

        String inputPath = args[0], outputPath = args[1];
        System.out.println("inputPath = " + inputPath);
        System.out.println("outputPath = " + outputPath);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondarySort");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(s -> {
            String[] tokens = s.split(",");
            System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
            Tuple2<Integer, Integer> timeValue = new Tuple2<>(
                    Integer.parseInt(tokens[1]),
                    Integer.parseInt(tokens[2]));
            return new Tuple2<>(tokens[0], timeValue);
        });

        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
    }
}
