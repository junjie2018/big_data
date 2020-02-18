package chapter5.pair;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import java.io.IOException;

import static util.WritableUtil.serialize;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntPairTest {

    private IntPair ip1 = new IntPair(1, 2);
    private IntPair ip2 = new IntPair(2, 1);
    private IntPair ip3 = new IntPair(1, 12);
    private IntPair ip4 = new IntPair(11, 2);
    private IntPair ip5 = new IntPair(Integer.MAX_VALUE, 2);
    private IntPair ip6 = new IntPair(Integer.MAX_VALUE, Integer.MAX_VALUE);

    @Test
    public void testComparator() throws IOException {
        check(ip1, ip1, 0);
        check(ip1, ip2, -1);
        check(ip3, ip4, -1);
        check(ip2, ip4, -1);
        check(ip3, ip5, -1);
        check(ip5, ip6, -1);
    }

    private void check(IntPair ip1, IntPair ip2, int c) throws IOException {
        check(WritableComparator.get(IntPair.class), ip1, ip2, c);
    }

    private void check(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
        checkOnce(comp, ip1, ip2, c);
        checkOnce(comp, ip2, ip1, -c);
    }

    @SuppressWarnings("unchecked")
    public void checkOnce(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
        assertThat("Object", signum(comp.compare(ip1, ip2)), is(c));
        byte[] out1 = WritableUtil.serialize(ip1);
        byte[] out2 = WritableUtil.serialize(ip2);
        assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
    }

    private int signum(int i) {
        return Integer.compare(i, 0);
    }


}