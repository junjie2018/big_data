package chap01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int numberOfPartitions) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
