package chap07;

import common.LocalTest;
import common.PathUtil;
import org.junit.Test;

public class MBADriverTest extends LocalTest {
    @Test
    public void test() throws Exception {
        runTest(new MBADriver(),
                "in:sample_input_shoppingcat.txt",
                "out:tmp",
                "2"
        );
    }
}
