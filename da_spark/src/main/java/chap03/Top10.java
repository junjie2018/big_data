package chap03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtil;

import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.List;
import java.util.Map;

public class Top10 {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: Top10 <input-file>");
            System.exit(1);
        }

        String inputPath = args[0];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("Top10");

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> {
            String[] tokens = s.split(",");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });

        JavaRDD<SortedMap<Integer, String>> partitions =
                pairs.mapPartitions((iter) -> {
                    SortedMap<Integer, String> top10 = new TreeMap<>();
                    while (iter.hasNext()) {
                        Tuple2<String, Integer> tuple = iter.next();
                        top10.put(tuple._2, tuple._1);
                        if (top10.size() > 10) {
                            top10.remove(top10.firstKey());
                        }
                    }
                    return Collections.singletonList(top10);
                });

        SortedMap<Integer, String> finalTop10 = new TreeMap<>();
        List<SortedMap<Integer, String>> allTop10 = partitions.collect();
        for (SortedMap<Integer, String> location10 : allTop10) {
            for (Map.Entry<Integer, String> entry : location10.entrySet()) {
                finalTop10.put(entry.getKey(), entry.getValue());
                if (finalTop10.size() > 10) {
                    finalTop10.remove(finalTop10.firstKey());
                }
            }
        }

        for (Map.Entry<Integer, String> entry : finalTop10.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }

        System.exit(0);
    }
}
