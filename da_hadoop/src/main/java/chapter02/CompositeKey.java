package chapter02;

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
    private String stockSymbol;
    private long timestamp;


    @Override
    public int compareTo(CompositeKey other) {
        if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(other.stockSymbol);
        } else if (this.timestamp != other.timestamp) {
            return Long.compare(timestamp, other.timestamp);
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.stockSymbol);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stockSymbol = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    public static class CompositeKeyComparator extends WritableComparator {
        public CompositeKeyComparator() {
            super(CompositeKey.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
    }
}
