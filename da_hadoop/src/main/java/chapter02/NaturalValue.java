package chapter02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import util.DateUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NaturalValue implements Writable, Comparable<NaturalValue> {

    private long timestamp;
    private double price;

    public static NaturalValue copy(NaturalValue value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public NaturalValue clone() {
        return new NaturalValue(timestamp, price);
    }


    @Override
    public int compareTo(NaturalValue data) {
        return Long.compare(this.timestamp, data.timestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(timestamp);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.price = dataInput.readDouble();
    }
}
