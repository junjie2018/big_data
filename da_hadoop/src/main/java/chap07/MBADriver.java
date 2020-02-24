package chap07;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import util.Constant;

public class MBADriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfPairs = Integer.parseInt(args[2]);

        Job job = Job.getInstance(getConf(), "MBADriver");
        job.getConfiguration().setInt(Constant.CHAP07_PAIRS, numberOfPairs);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MBAMapper.class);
        job.setCombinerClass(MBAReducer.class);
        job.setReducerClass(MBAReducer.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }
}
