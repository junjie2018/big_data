package chap01;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;


public class TupleComparatorDescending implements Serializable, Comparator<Tuple2<String, Integer>> {

    private static final long serialVersionUID = 1287049512718728895L;

    static final TupleComparatorDescending INSTANCE = new TupleComparatorDescending();

    private TupleComparatorDescending() {
    }

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return o2._1.compareTo(o1._1) != 0 ? o2._1.compareTo(o1._1) : o2._2.compareTo(o1._2);
    }
}
