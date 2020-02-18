package chap03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AggregateByKeyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text k2 = new Text();
    private IntWritable v2 = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valueAsString = value.toString().trim();
        String[] tokens = valueAsString.split(",");
        if (tokens.length != 2) {
            return;
        }

        String url = tokens[0];
        int frequency = Integer.parseInt(tokens[1]);
        k2.set(url);
        v2.set(frequency);
    }
}
