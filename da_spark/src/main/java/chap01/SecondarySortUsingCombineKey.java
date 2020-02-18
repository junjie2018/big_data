package chap01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.DataStructures;
import util.SparkUtil;

import java.util.SortedMap;
import java.util.TreeMap;

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

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("SecondarySortUsingCombineKey");

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(s -> {
            String[] tokens = s.split(",");
            System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
            Tuple2<Integer, Integer> timeValue = new Tuple2<>(
                    Integer.parseInt(tokens[1]),
                    Integer.parseInt(tokens[2]));
            return new Tuple2<>(tokens[0], timeValue);
        });

        JavaPairRDD<String, SortedMap<Integer, Integer>> combined = pairs.combineByKey(
                // createCombiner
                (Tuple2<Integer, Integer> x) -> {
                    SortedMap<Integer, Integer> map = new TreeMap<>();
                    map.put(x._1, x._2);
                    return map;
                },
                // mergeValue
                (SortedMap<Integer, Integer> map, Tuple2<Integer, Integer> x) -> {
                    map.put(x._1, x._2);
                    return map;
                },
                // mergeCombiners
                (SortedMap<Integer, Integer> map1, SortedMap<Integer, Integer> map2) -> {
                    if (map1.size() < map2.size()) {
                        return DataStructures.merge(map1, map2);
                    } else {
                        return DataStructures.merge(map1, map2);
                    }
                });

        combined.saveAsTextFile(outputPath);

        ctx.close();

        System.exit(0);
    }
}
