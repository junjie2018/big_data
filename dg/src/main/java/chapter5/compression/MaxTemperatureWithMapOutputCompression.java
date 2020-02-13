package chapter5.compression;

import chapter2.MaxTemperatureDriver;
import chapter2.MaxTemperatureMapper;
import chapter2.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTemperatureWithMapOutputCompression {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompression <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

        Job job = new Job(conf);
        // 这个地方有点奇奇怪怪
        job.setJarByClass(MaxTemperatureDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
