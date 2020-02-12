package da.chapter01.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator {
    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
