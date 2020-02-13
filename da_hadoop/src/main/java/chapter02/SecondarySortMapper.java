package chapter02;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DateUtil;


import java.io.IOException;
import java.util.Date;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
    private final CompositeKey reduceKey = new CompositeKey();
    private final NaturalValue reduceValue = new NaturalValue();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString().trim(), ",");
        if (tokens.length == 3) {
            Date date = DateUtil.getDate(tokens[1]);
            if (null == date) {
                return;
            }
            long timestamp = date.getTime();
            reduceKey.setStockSymbol(tokens[0]);
            reduceKey.setTimestamp(timestamp);

            reduceValue.setTimestamp(timestamp);
            reduceValue.setPrice(Double.parseDouble(tokens[2]));

            context.write(reduceKey, reduceValue);
        } else {
            // 忽略
        }
    }
}
