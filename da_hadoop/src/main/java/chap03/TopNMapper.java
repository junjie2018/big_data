package chap03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNMapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {

    private int N = 10;
    private SortedMap<Integer, String> top = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueAsString = value.toString().trim();
        String[] tokens = valueAsString.split(",");

        String cat = tokens[0];
        int frequency = Integer.parseInt(tokens[1]);

        String compositeValue = cat + "," + frequency;
        top.put(frequency, compositeValue);
        if (top.size() > N) {
            top.remove(top.firstKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String value : top.values()) {
            context.write(NullWritable.get(), new Text(value));
        }
    }
}
