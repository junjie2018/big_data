package chap05;

import edu.umd.cloud9.io.pair.PairOfStrings;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PairOfWords implements WritableComparable<PairOfWords> {
    private String leftElement;
    private String rightElement;

    @Override
    public int compareTo(PairOfWords pair) {
        String pl = pair.getLeftElement();
        String pr = pair.getRightElement();

        return leftElement.equals(pl) ?
                rightElement.compareTo(pr) :
                leftElement.compareTo(pl);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, leftElement);
        Text.writeString(dataOutput, rightElement);
    }

    public void setWord(String leftElement) {
        setLeftElement(leftElement);
    }

    public String getWord() {
        return leftElement;
    }

    public void setNeighbor(String rightElement) {
        setRightElement(rightElement);
    }

    public String getNeighbor() {
        return rightElement;
    }

    public String getKey() {
        return leftElement;
    }

    public String getValue() {
        return rightElement;
    }

    public void set(String left, String right) {
        leftElement = left;
        rightElement = right;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        leftElement = Text.readString(dataInput);
        rightElement = Text.readString(dataInput);
    }

    @Override
    public int hashCode() {
        return leftElement.hashCode() + rightElement.hashCode();
    }

    @Override
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    @Override
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    protected Object clone() throws CloneNotSupportedException {
        return new PairOfWords(this.leftElement, this.rightElement);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PairOfWords)) {
            return false;
        } else {
            PairOfWords pair = (PairOfWords) obj;
            return leftElement.equals(pair.getLeftElement())
                    && rightElement.equals(pair.getRightElement());
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PairOfWords.class);
        }

        @Override
        // 这个地方用的应该是递归比较
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int firstStrL1 = readVInt(b1, s1);
                int firstStrL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1,
                        b2, s2 + firstVIntL2, firstStrL2);
                if (cmp != 0) {
                    return cmp;
                }

                int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1,
                        b2, s2 + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        static {
            WritableComparator.define(PairOfWords.class, new Comparator());
        }
    }
}
