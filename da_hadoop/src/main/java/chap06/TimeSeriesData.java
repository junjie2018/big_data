package chap06;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import util.DateUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {

    private long timestamp;
    private double value;

    public static TimeSeriesData copy(TimeSeriesData tsd) {
        return new TimeSeriesData(tsd.timestamp, tsd.value);
    }

    public static TimeSeriesData read(DataInput in) throws IOException {
        TimeSeriesData tsData = new TimeSeriesData();
        tsData.readFields(in);
        return tsData;
    }

    public void set(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesData data) {
        return Long.compare(this.timestamp, data.timestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.value = dataInput.readDouble();
    }

    public String getDate() {
        return DateUtil.getDateAsString(timestamp);
    }

    @Override
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public TimeSeriesData clone() {
        return new TimeSeriesData(timestamp, value);
    }

    public String toString() {
        return String.format("(%d, %f)", timestamp, value);
    }
}
