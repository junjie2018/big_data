package chap04;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {
    @Override
    // 这种写法没有在性能上做任何提升
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataInputBuffer buffer = new DataInputBuffer();
        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();

        try {
            buffer.reset(b1, s1, l1);
            a.readFields(buffer);
            buffer.reset(b2, s2, l2);
            b.readFields(buffer);
            return compare(a, b);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int compare(PairOfStrings first, PairOfStrings second) {
        return first.getLeftElement().compareTo(second.getLeftElement());
    }
}
