package chap08;

import common.LocalTest;
import org.junit.Test;

public class CommonFriendsDriverTest extends LocalTest {
    @Test
    public void test() throws Exception {
        runTest(new CommonFriendsDriver(),
                "in:sample_input_friends.txt",
                "out:tmp");
    }

    @Test
    public void test2() throws Exception {
        runTest(new CommonFriendsUsingListDriver(),
                "in:sample_input_friends.txt",
                "out:tmp");
    }
}
