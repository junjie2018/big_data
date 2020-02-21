package chap06.memorysort;

import chap06.TimeSeriesData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DateUtil;

import java.io.IOException;
import java.util.Date;

public class SortInMemory_MovingAverageMapper extends Mapper<LongWritable, Text, Text, TimeSeriesData> {

    private final Text reducerKey = new Text();
    private final TimeSeriesData reduceValue = new TimeSeriesData();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        if (record == null || record.length() == 0) {
            return;
        }

        String[] tokens = StringUtils.split(record.trim(), ",");
        if (tokens.length != 3) {
            return;
        }

        Date date = DateUtil.getDate(tokens[1]);
        if (date == null) {
            return;
        }

        reducerKey.set(tokens[0]);
        reduceValue.set(date.getTime(), Double.parseDouble(tokens[2]));
        context.write(reducerKey, reduceValue);
    }
}
