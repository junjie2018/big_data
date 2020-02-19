package chap04;

import common.LocalTest;
import org.junit.Test;

public class LeftJoinTest extends LocalTest {

    @Test
    public void test() throws Exception {
        runTest(new LeftJoinDriver(),
                "in:transactions.txt",
                "in:user.txt",
                "out:tmp");
    }

    @Test
    public void test2() throws Exception {
        runTest(new LocationCountDriver(),
                "in:user_transactions.txt",
                "out:tmp");
    }

}
