package chap04;

import common.PathUtil;
import org.junit.Test;

public class SparkLeftOuterJoinTest {
    @Test
    public void test() {
        SparkLeftOuterJoinScala.main(new String[]{
                PathUtil.getInputPath("user.txt"),
                PathUtil.getInputPath("transactions.txt")
        });
    }
}
