package chap05;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Option;
import scala.Tuple2;
import util.SparkUtil;

import java.util.*;

public class RelativeFrequency {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: RelativeFrequencyJava <neighbor-window> <input-dir> <output-dir>");
            System.exit(1);
        }

        JavaSparkContext sc = SparkUtil.createLocalJavaSparkContext("RelativeFrequency");

        int neighborWindow = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        final Broadcast<Integer> broadcastWindow = sc.broadcast(neighborWindow);

        JavaRDD<String> rawData = sc.textFile(input);

        // pairs最好加一个持久化，因为接下来需要两次用到这个
        JavaPairRDD<String, Tuple2<String, Integer>> pairs = rawData.flatMapToPair(line -> {
            String[] tokens = line.split("\\s+");

            List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
            for (int i = 0; i < tokens.length; i++) {
                int start = (i - broadcastWindow.value() < 0) ? 0 : i - broadcastWindow.value();
                int end = (i + broadcastWindow.value() >= tokens.length) ? tokens.length - 1 : i + broadcastWindow.value();
                for (int j = start; j <= end; j++) {
                    if (j == i) {
                        continue;
                    }
                    list.add(new Tuple2<>(tokens[i], new Tuple2<>(tokens[j], 1)));
                }
            }
            return list.iterator();
        });

        // 这个用来统计边缘计数
        JavaPairRDD<String, Integer> totalByKey = pairs
                .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2))
                .reduceByKey(Integer::sum);

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> grouped = pairs.groupByKey();
        // flatMapValues有点意思，和我想象的返回结果不一样
        JavaPairRDD<String, Tuple2<String, Integer>> uniquePairs = grouped.flatMapValues(values -> {
            Map<String, Integer> map = new HashMap<>();
            for (Tuple2<String, Integer> value : values) {
                map.put(value._1, map.getOrDefault(value._1, 0) + value._2);
            }

            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                list.add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
            return list;
        });

        /*
            todo 可能是版本的问题，我暂时无法修复这个问题
            todo 我计划升级个版本，顺便完成下之前的实现
         */
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joined = uniquePairs.join(totalByKey);
        JavaPairRDD<Tuple2<String, String>, Double> relativeFrequency = joined.mapToPair(tuple ->
                new Tuple2<>(
                        new Tuple2<>(tuple._1, tuple._2._1._1),
                        ((double) tuple._2._1._2 / tuple._2._2))

        );
        JavaRDD<String> formatResult_tab_separated =
                relativeFrequency.map(tuple -> tuple._1._1 + "\t" + tuple._1._2 + tuple._2);


        formatResult_tab_separated.saveAsTextFile(output);
    }
}
