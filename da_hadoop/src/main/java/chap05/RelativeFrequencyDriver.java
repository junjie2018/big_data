package chap05;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

@Slf4j
public class RelativeFrequencyDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        int neighborWindow = Integer.parseInt(args[0]);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "RelativeFrequencyDriver");
        job.setJarByClass(RelativeFrequencyDriver.class);

        // 代码中根本没有用到这个变量
        job.getConfiguration().setInt("neighbor.window", neighborWindow);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(PairOfWords.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setCombinerClass(RelativeFrequencyCombiner.class);
        job.setPartitionerClass(OrderInversionPartitioner.class);
        job.setNumReduceTasks(3); // 会产生三个文件

        boolean status = job.waitForCompletion(true);
        log.info("run(): status = " + status);
        return status ? 0 : 1;
    }
}
