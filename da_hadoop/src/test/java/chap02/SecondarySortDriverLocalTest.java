package chap02;

import common.LocalTest;
import org.junit.Test;


public class SecondarySortDriverLocalTest extends LocalTest {
    @Test
    public void testSecondarySortDriver() throws Exception {
        runTest(new SecondarySortDriver(), "in:sample_input2.txt", "tmp");
    }
}
