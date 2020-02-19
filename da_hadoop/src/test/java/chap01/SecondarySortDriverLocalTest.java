package chap01;

import common.LocalTest;
import org.junit.Test;


public class SecondarySortDriverLocalTest extends LocalTest {
    @Test
    public void testSecondarySortDriver() throws Exception {
        runTest(new SecondarySortDriver(), "in:sample_input.txt", "out:tmp");
    }
}
