package chap06.secondarysort;

import chap06.TimeSeriesData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DateUtil;

import java.io.IOException;
import java.util.Date;

public class SortByMRF_MobingAverageMapper
        extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesData> {

    private final CompositeKey reduceKey = new CompositeKey();
    private final TimeSeriesData reduceValue = new TimeSeriesData();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        if (record == null || record.length() == 0) {
            return;
        }

        String[] tokens = StringUtils.split(record, ",");
        if (tokens.length != 3) {
            return;
        }

        Date date = DateUtil.getDate(tokens[1]);
        if (date == null) {
            return;
        }

        long timestamp = date.getTime();
        reduceKey.set(tokens[0], timestamp);
        reduceValue.set(timestamp, Double.parseDouble(tokens[2]));

        context.write(reduceKey, reduceValue);
    }
}
