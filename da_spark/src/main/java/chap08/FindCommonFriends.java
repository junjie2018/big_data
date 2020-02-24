package chap08;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtil;

import java.util.*;

public class FindCommonFriends {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: FindCommonFriends <input-file> <output-file>");
            System.exit(1);
        }

        String input = args[0], output = args[1];

        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("FindCommonFriends");

        JavaRDD<String> records = ctx.textFile(input, 1);

        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> pairs = records.flatMapToPair(s -> {
            String[] tokens = s.split(",");
            long person = Long.parseLong(tokens[0]);
            String friendsAsString = tokens[1];
            String[] friendTokenizend = friendsAsString.split(" ");
            if (friendTokenizend.length == 1) {
                Tuple2<Long, Long> key = buildSortedTuple(person, Long.parseLong(friendTokenizend[0]));
                return Arrays.asList(new Tuple2<Tuple2<Long, Long>, Iterable<Long>>(key, new ArrayList<>())).iterator();
            }

            List<Long> friends = new ArrayList<>();
            for (String f : friendTokenizend) {
                friends.add(Long.parseLong(f));
            }

            List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> result = new ArrayList<>();
            for (Long friend : friends) {
                Tuple2<Long, Long> key = buildSortedTuple(person, friend);
                result.add(new Tuple2<>(key, friends));
            }
            return result.iterator();
        });

        JavaPairRDD<Tuple2<Long, Long>, Iterable<Iterable<Long>>> grouped = pairs.groupByKey();

        JavaPairRDD<Tuple2<Long, Long>, List<Long>> commonFriends = grouped.mapValues(s -> {
            Map<Long, Integer> countCommon = new HashMap<>();
            int size = 0;
            for (Iterable<Long> iterable : s) {
                size++;
                List<Long> list = iterableToList(iterable);
                if (list == null || list.isEmpty()) {
                    continue;
                }
                for (Long f : list) {
                    countCommon.put(f, countCommon.getOrDefault(f, 0) + 1);
                }
            }

            List<Long> finalCommonFriends = new ArrayList<>();
            for (Map.Entry<Long, Integer> entry : countCommon.entrySet()) {
                if (entry.getValue() == size) {
                    finalCommonFriends.add(entry.getKey());
                }
            }
            return finalCommonFriends;
        });

        commonFriends.saveAsTextFile(output);

        ctx.close();
    }

    private static Tuple2<Long, Long> buildSortedTuple(long a, long b) {
        if (a < b) {
            return new Tuple2<>(a, b);
        } else {
            return new Tuple2<>(b, a);
        }
    }

    private static List<Long> iterableToList(Iterable<Long> iterable) {
        List<Long> list = new ArrayList<>();
        for (Long item : iterable) {
            list.add(item);
        }
        return list;
    }
}
