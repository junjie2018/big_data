package chap03;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import util.SparkUtil;

import java.util.*;

public class Top10NonUnique {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Top10 <topN> <input-path>");
            System.exit(1);
        }

        final int N = Integer.parseInt(args[0]);
        final String inputPath = args[1];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("Top10NonUnique");

        Broadcast<Integer> topN = ctx.broadcast(N);

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        // todo 了解一下这个方法是做什么用的
        JavaRDD<String> rdd = lines.coalesce(9);

        JavaPairRDD<String, Integer> kv = rdd.mapToPair(s -> {
            String[] tokens = s.split(",");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });

        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(Integer::sum);

        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions(iter -> {
            final int n = topN.value();
            SortedMap<Integer, String> localTopN = new TreeMap<>();
            while (iter.hasNext()) {
                Tuple2<String, Integer> tuple = iter.next();
                localTopN.put(tuple._2, tuple._1);
                if (localTopN.size() > N) {
                    localTopN.remove(localTopN.firstKey());
                }
            }
            return Collections.singletonList(localTopN);
        });

        SortedMap<Integer, String> finalTopN = new TreeMap<>();
        List<SortedMap<Integer, String>> allTopN = partitions.collect();
        for (SortedMap<Integer, String> localTopN : allTopN) {
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
                finalTopN.put(entry.getKey(), entry.getValue());
                if (finalTopN.size() > N) {
                    finalTopN.remove(finalTopN.firstKey());
                }
            }
        }

        for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }

        System.exit(0);
    }
}
