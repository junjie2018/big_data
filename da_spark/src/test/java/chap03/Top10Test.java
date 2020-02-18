package chap03;

import common.PathUtil;
import org.junit.Test;
import util.CommonUtil;

public class Top10Test {
    @Test
    public void test() {
        Top10.main(PathUtil.getPath("sample_input_cat.txt", ""));
    }

    @Test
    public void test2() {
        Top10NonUnique.main(CommonUtil.combineArray(
                new String[]{"3"},
                PathUtil.getPath("sample_input_url.txt", "")
        ));
    }

    @Test
    public void test3() {
        Top10UsingTakeOrdered.main(CommonUtil.combineArray(
                new String[]{"3"},
                PathUtil.getPath("sample_input_url.txt", "")
        ));
    }
}
