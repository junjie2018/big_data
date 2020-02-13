package chapter02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey ck1 = (CompositeKey) a;
        CompositeKey ck2 = (CompositeKey) b;

        int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
        return comparison != 0 ? comparison : Long.compare(ck1.getTimestamp(), ck2.getTimestamp());
    }
}
