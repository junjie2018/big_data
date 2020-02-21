package chap05;

import common.LocalTest;
import org.junit.Test;

public class RelativeFrequencyDriverTest extends LocalTest {
    @Test
    public void test() throws Exception {
        runTest(new RelativeFrequencyDriver(),
                "2",
                "in:sample_input_word.txt",
                "out:tmp");
    }
}
