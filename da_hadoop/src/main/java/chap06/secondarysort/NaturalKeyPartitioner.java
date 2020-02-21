package chap06.secondarysort;

import chap06.TimeSeriesData;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData> {
    @Override
    public int getPartition(CompositeKey compositeKey, TimeSeriesData timeSeriesData, int numberOfPartitions) {
        return Math.abs((int) (hash(compositeKey.getName()) % numberOfPartitions));
    }

    private long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
