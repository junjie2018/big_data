package chapter01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class SecondarySortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "SecondarySortDriver");
        job.setJarByClass(SecondarySortDriver.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        /*
            分组比较器默认调用的是key的比较器，这个地方其实没有必要重写。
         */
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
//        theLogger.info("run(): status=" + status);
        return status ? 0 : 1;
    }
}
