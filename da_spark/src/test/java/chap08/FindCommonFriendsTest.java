package chap08;

import common.PathUtil;
import org.junit.Test;

public class FindCommonFriendsTest {
    @Test
    public void test() {
        FindCommonFriends.main(new String[]{
                PathUtil.getInputPath("sample_input_friends.txt"),
                PathUtil.getOutputPath("tmp")
        });
    }
}
