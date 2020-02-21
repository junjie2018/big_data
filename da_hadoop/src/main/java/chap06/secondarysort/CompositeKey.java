package chap06.secondarysort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompositeKey implements WritableComparable<CompositeKey> {

    private String name;
    private long timestamp;

    public void set(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey other) {
        return this.name.compareTo(other.name) != 0 ?
                this.name.compareTo(other.name) :
                Long.compare(this.timestamp, other.timestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    public static class CompositeKeyComparator extends WritableComparator {
        public CompositeKeyComparator(){
            super(CompositeKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(CompositeKey.class,new CompositeKeyComparator());
    }
}
