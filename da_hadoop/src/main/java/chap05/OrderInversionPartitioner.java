package chap05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords pair, IntWritable writable, int numberOfPartitions) {
        String leftWord = pair.getLeftElement();
        return Math.abs((int) hash(leftWord) % numberOfPartitions);
    }

    private static long hash(String str) {
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
