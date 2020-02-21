package chap06.memorysort;

import chap06.TimeSeriesData;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import util.Constant;

@Slf4j
public class SortInMemory_MovingAverageDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SortInMemory_MovingAverageDriver <window_size> <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(getConf(), "SortInMemory_MovingAverageDriver");

        job.setMapperClass(SortInMemory_MovingAverageMapper.class);
        job.setReducerClass(SortInMemory_MovingAverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        int windowSize = Integer.parseInt(args[0]);
        job.getConfiguration().setInt(Constant.CHAP06_WINDOW_SIZE, windowSize);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean status = job.waitForCompletion(true);
        log.info("run(): status = " + status);
        return status ? 0 : 1;
    }
}
