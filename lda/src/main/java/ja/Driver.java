package ja;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Driver {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取输入数据
        JavaRDD<String> input = sc.textFile("C:\\Users\\Junjie\\Desktop\\big_data\\src\\main\\java\\lda\\ja.Driver.java");

        // 切分为单词
        JavaRDD<String> words = input.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));

        // 转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
        counts.saveAsTextFile("C:\\Users\\Junjie\\Desktop\\data\\tmp");
    }
}
