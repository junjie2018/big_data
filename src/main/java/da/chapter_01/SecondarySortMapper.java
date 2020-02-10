package da.chapter_01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {
    private final Text theTemprature = new Text();
    private final DateTemperaturePair pair = new DateTemperaturePair();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        int temprature = Integer.parseInt(tokens[3]);



        super.map(key, value, context);
    }
}
