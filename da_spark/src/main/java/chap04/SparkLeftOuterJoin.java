package chap04;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SparkLeftOuterJoin {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkLeftOuterJoin <users> <transactions>");
            System.exit(1);
        }

        String usersInputFile = args[0];
        String transactionsInputFile = args[1];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("SparkLeftOuterJoin");

        JavaRDD<String> users = ctx.textFile(usersInputFile, 1);
        JavaPairRDD<String, Tuple2<String, String>> usersRDD = users.mapToPair(s -> {
            String[] userRecord = s.split(" ");
            Tuple2<String, String> location = new Tuple2<>("L", userRecord[1]);
            return new Tuple2<>(userRecord[0], location);
        });

        JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);
        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD = transactions.mapToPair(s -> {
            String[] transactionRecord = s.split(" ");
            Tuple2<String, String> product = new Tuple2<>("P", transactionRecord[1]);
            return new Tuple2<>(transactionRecord[2], product);
        });

        // 截止目前还是以userId进行关联的
        JavaPairRDD<String, Tuple2<String, String>> allRDD = transactionsRDD.union(usersRDD);
        // 一个userId的location与购买的productId被放在了同一个Iterable中进行下一步处理
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();

        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair(s -> {
            Iterable<Tuple2<String, String>> pairs = s._2;
            String location = "UNKNOWN";
            List<String> products = new ArrayList<>();

            for (Tuple2<String, String> t2 : pairs) {
                if (t2._1.equals("L")) {
                    location = t2._2;
                } else {
                    products.add(t2._2);
                }
            }

            List<Tuple2<String, String>> kvList = new ArrayList<>();
            for (String product : products) {
                kvList.add(new Tuple2<>(product, location));
            }

            // flatMapToPair的确在这个地方对kvList进行了一次展开
            return kvList;
        });

        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productByLocations.mapValues(s -> {
            Set<String> uniqueLocations = new HashSet<>();
            for (String location : s) {
                uniqueLocations.add(location);
            }
            return new Tuple2<>(uniqueLocations, uniqueLocations.size());
        });

        System.out.println("=== Unique Locations and Counts ===");
        List<Tuple2<String, Tuple2<Set<String>, Integer>>> debug4 = productByUniqueLocations.collect();
        System.out.println("--- debug4 begin ---");
        for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
            System.out.println("debug4 t2._1=" + t2._1);
            System.out.println("debug4 t2._2=" + t2._2);
        }
        System.out.println("--- debug4 end ---");
        System.exit(0);
    }
}
