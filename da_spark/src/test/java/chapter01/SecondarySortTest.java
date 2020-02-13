package chapter01;


import common.PathUtil;
import org.junit.Test;

public class SecondarySortTest {
    @Test
    public void test() {
        SecondarySortUsingGroupByKey.main(PathUtil.getPath("sample_input.txt", "tmp2"));
    }

    @Test
    public void test2(){
        SecondarySortUsingCombineKey.main(PathUtil.getPath("sample_input.txt", "tmp3"));
    }
}
