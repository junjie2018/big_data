package chap07;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.annotation.meta.param;
import util.Combination;
import util.SparkUtil;

import java.util.ArrayList;
import java.util.List;

public class FindAssociationRules {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: FindAssociationRules <transactions> <output>");
            System.exit(1);
        }

        String transactionFileName = args[0];
        String output = args[1];
        JavaSparkContext ctx = SparkUtil.createLocalJavaSparkContext("FindAssociationRules");

        JavaRDD<String> transactions = ctx.textFile(transactionFileName, 1);
        JavaPairRDD<List<String>, Integer> patterns = transactions.flatMapToPair(transaction -> {
            List<String> list = Util.toList(transaction);
            List<List<String>> combinations = Combination.findSortedCombinations(list);
            List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
            for (List<String> combinationList : combinations) {
                if (combinationList.size() > 0) {
                    result.add(new Tuple2<>(combinationList, 1));
                }
            }
            return result.iterator();
        });

        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(Integer::sum);

        // *****************************************************************************
        // 截止到目前，已经可以得到什么呢？可以得到每个组合的次数，还需要什么呢？还需要找到
        // 所有可能出现的关联规则的次数，因为只有这样，才能计算出X->Y的次数，占所有X出现的次
        // 数的比例
        // *****************************************************************************


        /*
         * 该步操作的所有结果如下：
         *          [{a, b, c, d, e}, [null, N]]
         *          [{b, c, d, e}, [{a, b, c, d, e}，N]]
         *          [{a, c, d, e}, [{a, b, c, d, e}，N]]
         *          [{a, b, d, e}, [{a, b, c, d, e}，N]]
         *          [{a, b, c, e}, [{a, b, c, d, e}，N]]
         *          [{a, b, c, d}, [{a, b, c, d, e}，N]]
         */
        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subPatterns = combined.flatMapToPair(pattern -> {
            List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<>();

            List<String> curList = pattern._1;
            Integer frequency = pattern._2;

            result.add(new Tuple2<>(curList, new Tuple2<>(null, frequency)));
            if (curList.size() == 1) {
                return result.iterator();
            }

            /*
             * 理解如下：
             *      如果一个集合为{a, b, c, d, e}，该处理过后，结果为：
             *          {b, c, d, e},{a, c, d, e},{a, b, d, e},{a, b, c, e},{a, b, c, d}
             *      但是最终写到RDD中的结果为：
             *          [{b, c, d, e}, [{a, b, c, d, e}，N]]
             *          [{a, c, d, e}, [{a, b, c, d, e}，N]]
             *          [{a, b, d, e}, [{a, b, c, d, e}，N]]
             *          [{a, b, c, e}, [{a, b, c, d, e}，N]]
             *          [{a, b, c, d}, [{a, b, c, d, e}，N]]
             */
            for (int i = 0; i < curList.size(); i++) {
                List<String> sublist = Util.removeOneItem(curList, i);
                result.add(new Tuple2<>(sublist, new Tuple2<>(curList, frequency)));
            }
            return result.iterator();
        });

        // 这一块后面需要好好理解一下
        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subPatterns.groupByKey();

        // 该步处理后，接下的处理工作就比较简单了
        JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> assocRules = rules.map(item -> {
            /*
                该lambda处理的数据为：
                    [{a,b,c},[[{null}, N],[{a,b,c,d}, N],[{a,b,c,e}, N],[{a,b,c,f}, N],[{a,b,c,g}, N]]
             */
            List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<>();

            List<String> xSet = item._1;
            Iterable<Tuple2<List<String>, Integer>> xAndYSets = item._2;

            Tuple2<List<String>, Integer> xSetCounts = null;
            List<Tuple2<List<String>, Integer>> xAndYSetCounts = new ArrayList<>();

            for (Tuple2<List<String>, Integer> xAndYSet : xAndYSets) {
                if (xAndYSet._1 == null) {
                    xSetCounts = xAndYSet;
                } else {
                    xAndYSetCounts.add(xAndYSet);
                }
            }

            if (xAndYSetCounts.isEmpty()) {
                return result;
            }

            for (Tuple2<List<String>, Integer> t2 : xAndYSetCounts) {
                // 计算出x商品集合和y商品集合同时存在的订单，占x商品集合存在的订单的比例
                @SuppressWarnings("ConstantConditions")
                double confidence = (double) t2._2 / (double) xSetCounts._2;

                List<String> ySet = new ArrayList<>(t2._1);
                ySet.removeAll(xSet);
                result.add(new Tuple3<>(xSet, ySet, confidence));
            }
            return result;
        });

        assocRules.saveAsTextFile(output);

        ctx.close();
        System.exit(0);
    }
}
