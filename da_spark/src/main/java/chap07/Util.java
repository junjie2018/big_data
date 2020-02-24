package chap07;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Util {
    static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        return new ArrayList<>(Arrays.asList(items));
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if (list == null || list.isEmpty()) {
            return list;
        }

        if (i < 0 || i >= list.size()) {
            return list;
        }

        List<String> cloned = new ArrayList<>(list);
        cloned.remove(i);

        return cloned;
    }
}
