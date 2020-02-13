package chapter8.partition;

import util.NcdcRecordParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StationPartitioner extends Partitioner<LongWritable, Text> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public int getPartition(LongWritable longWritable, Text text, int i) {
        parser.parse(text);
        return getPartition(parser.getStationId());
    }

    private int getPartition(String stationId) {
        return 0;
    }
}
