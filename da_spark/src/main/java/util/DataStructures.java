package util;

import java.util.SortedMap;

public class DataStructures {
    public static SortedMap<Integer, Integer> merge(
            final SortedMap<Integer, Integer> smaller,
            final SortedMap<Integer, Integer> larger) {
        for (Integer key : smaller.keySet()) {
            Integer valueFromLargeMap = larger.get(key);
            if (valueFromLargeMap == null) {
                larger.put(key, smaller.get(key));
            } else {
                int mergedValue = valueFromLargeMap + smaller.get(key);
                larger.put(key, mergedValue);
            }
        }
        return larger;
    }
}
