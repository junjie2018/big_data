package chap04;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
    private PairOfStrings outputKey = new PairOfStrings();
    private PairOfStrings outputValue = new PairOfStrings();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");
        String productId = tokens[1];
        String userId = tokens[2];
        outputKey.set(userId, "2");
        outputValue.set("P", productId);
        context.write(outputKey, outputValue);
    }
}
