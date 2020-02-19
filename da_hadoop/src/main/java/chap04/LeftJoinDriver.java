package chap04;

import edu.umd.cloud9.io.pair.PairOfStrings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

@Slf4j
public class LeftJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {

    }

    @Override
    public int run(String[] args) throws Exception {
        Path transactions = new Path(args[0]);
        Path users = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "Phase-1: Left Outer Join");
        job.setJarByClass(LeftJoinDriver.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        // reducer
        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // mapper
        MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        FileOutputFormat.setOutputPath(job, output);

        boolean status = job.waitForCompletion(true);
        log.info("run(): status = " + status);
        return status ? 0 : 1;
    }
}
