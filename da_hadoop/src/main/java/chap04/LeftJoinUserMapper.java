package chap04;

import edu.umd.cloud9.io.pair.Pair;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
    private PairOfStrings outputKey = new PairOfStrings();
    private PairOfStrings outputValue = new PairOfStrings();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");

        // 这个地方不是很理解
        outputKey.set(tokens[0], "1");           // set user_id
        outputValue.set("L", tokens[1]);         // set location_id
        context.write(outputKey, outputValue);
    }
}
