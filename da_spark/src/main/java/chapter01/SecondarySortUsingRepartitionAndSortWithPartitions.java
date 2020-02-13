package chapter01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SecondarySortUsingRepartitionAndSortWithPartitions {
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

        // 转换为： JavaPairRDD<String, Tuple2<Integer, Integer>>
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(s -> {
            String[] tokens = s.split(",");
            System.out.println(Arrays.asList(tokens));
            Tuple2<Integer, Integer> timeValue = new Tuple2<>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
            return new Tuple2<>(tokens[0], timeValue);
        });

        // 输出pairs，便于测试
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2<String, Tuple2<Integer, Integer>> t : output) {
            System.out.println(t._1 + "," + t._2._1 + "," + t._2._2);
        }

        // 转化为：JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>>
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        // 输出groups，便于测试
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output2) {
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : t._2) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        // 内存中进行排序
        JavaPairRDD<String, ArrayList<Tuple2<Integer, Integer>>> sorted = groups.mapValues(s -> {
            ArrayList<Tuple2<Integer, Integer>> newList = iterableToList(s);
            newList.sort((t1, t2) -> t1._1.compareTo(t2._1));
            return newList;
        });

        // 输出sorted，便于测试
        List<Tuple2<String, ArrayList<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2<String, ArrayList<Tuple2<Integer, Integer>>> t : output3) {
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : t._2) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        sorted.saveAsTextFile(outputPath);
        System.exit(0);
    }

    static ArrayList<Tuple2<Integer, Integer>> iterableToList(Iterable<Tuple2<Integer, Integer>> iterable) {
        ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<>();
        for (Tuple2<Integer, Integer> item : iterable) {
            list.add(item);
        }
        return list;
    }
}
