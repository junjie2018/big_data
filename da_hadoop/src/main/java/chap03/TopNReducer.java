package chap03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
    private int N = 10;
    private SortedMap<Integer, String> top = new TreeMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String valueAsString = value.toString().trim();
            String[] tokens = valueAsString.split(",");
            String cat = tokens[0];
            int frequency = Integer.parseInt(tokens[1]);
            top.put(frequency, cat);
            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }

        for (Map.Entry<Integer, String> entry : top.entrySet()) {
            context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10);
    }
}
