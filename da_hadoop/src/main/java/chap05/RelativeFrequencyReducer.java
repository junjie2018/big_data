package chap05;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyReducer
        extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {
    // 通过二次排序，可以让(Word,*)被放置在首位置
    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NOT_DEFINED";

    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getNeighbor().equals("*")) {
            if (key.getWord().equals(currentWord)) {
                totalCount += totalCount + getTotalCount(values);
            } else {
                totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            relativeCount.set((double) count / totalCount);
            context.write(key, relativeCount);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
