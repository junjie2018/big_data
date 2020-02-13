package chapter01;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();

    public DateTemperaturePair(String yearMonth, String day, int temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        return -1 * compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        day.write(dataOutput);
        temperature.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth.readFields(dataInput);
        day.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString() + day.toString());
    }

    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    @Override
    @SuppressWarnings({"ConstantConditions", "RedundantIfStatement"})
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        DateTemperaturePair that = (DateTemperaturePair) obj;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }
        return true;
    }
}
