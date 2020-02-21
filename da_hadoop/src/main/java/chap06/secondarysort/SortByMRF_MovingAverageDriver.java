package chap06.secondarysort;

import chap06.TimeSeriesData;
import chap06.memorysort.SortInMemory_MovingAverageMapper;
import chap06.memorysort.SortInMemory_MovingAverageReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import util.Constant;

import javax.xml.soap.Text;

public class SortByMRF_MovingAverageDriver extends Configured implements Tool {
    @SuppressWarnings("Duplicates")
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SortByMRF_MovingAverageDriver <window_size> <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(getConf(), "SortByMRF_MovingAverageDriver");
        job.setMapperClass(SortByMRF_MobingAverageMapper.class);
        job.setReducerClass(SortByMRF_MovingAverageReducer.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        int windowSize = Integer.parseInt(args[0]);
        job.getConfiguration().setInt(Constant.CHAP06_WINDOW_SIZE, windowSize);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }
}
