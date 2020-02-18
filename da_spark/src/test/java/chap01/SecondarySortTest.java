package chap01;


import common.PathUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SecondarySortTest {
    @Test
    public void test() {
        SecondarySortUsingGroupByKey.main(PathUtil.getPath("sample_input.txt", "tmp2"));
    }

    @Test
    public void test2() {
        SecondarySortUsingCombineKey.main(PathUtil.getPath("sample_input.txt", "tmp3"));
    }

    @Test
    public void test3() {
        List<String> args = new ArrayList<>();
        args.add("4");
        args.addAll(Arrays.asList(PathUtil.getPath("sample_input.txt", "tmp4")));

        String[] argsArr=new String[3];
        args.toArray(argsArr);

        SecondarySortUsingRepartitionAndSortWithPartitions.main(argsArr);
    }
}
