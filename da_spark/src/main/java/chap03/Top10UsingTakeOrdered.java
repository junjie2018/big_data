package chap03;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtil;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Top10UsingTakeOrdered implements Serializable {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: Top10UsingTakeOrdered <topN> <input-path>");
            System.exit(1);
        }

        final int N = Integer.parseInt(args[0]);
        final String inputPath = args[1];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("Top10UsingTakeOrdered");

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        JavaRDD<String> rdd = lines.coalesce(9);
        JavaPairRDD<String, Integer> kv = rdd.mapToPair(s -> {
            String[] tokens = s.split(",");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> topNResult = uniqueKeys.takeOrdered(N, MyTupleComparator.INSTANCE);
        for (Tuple2<String, Integer> entry : topNResult) {
            System.out.println(entry._2 + "--" + entry._1);
        }

        System.exit(0);
    }

    static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o1._2.compareTo(o2._2);
        }
    }
}
