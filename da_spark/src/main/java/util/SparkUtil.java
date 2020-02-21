package util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtil {
    public static JavaSparkContext createJavaSparkContext() {
        return new JavaSparkContext();
    }

    public static JavaSparkContext createJavaSparkContext(
            String sparkMasterURL,
            String applicationName) {
        return new JavaSparkContext(sparkMasterURL, applicationName);
    }

    public static JavaSparkContext createJavaSparkContext(String applicationName) {
        SparkConf conf = new SparkConf().setAppName(applicationName);
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext createLocalJavaSparkContext(String applicationName) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(applicationName);
        return new JavaSparkContext(conf);
    }
}
