package chap06.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    private CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        return key1.getName().compareTo(key2.getName()) != 0 ?
                key1.getName().compareTo(key2.getName()) :
                Long.compare(key1.getTimestamp(), key2.getTimestamp());
    }
}
