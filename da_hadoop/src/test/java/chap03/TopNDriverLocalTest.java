package chap03;

import common.LocalTest;
import org.junit.Test;

public class TopNDriverLocalTest extends LocalTest {
    @Test
    // 不运行该测试，map中的写法并不正确
    public void testTopNDriver() throws Exception {
        runTest(new TopNDriver(), "5", "in:sample_input_cat.txt", "out:tmp");
    }
}
