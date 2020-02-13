package chapter01;


import common.PathUtil;
import org.junit.Test;

public class SecondarySortUsingGroupByKeyTest {
    @Test
    public void test() {
        SecondarySortUsingGroupByKey.main(PathUtil.getPath("sample_input.txt", "tmp2"));
    }
}
