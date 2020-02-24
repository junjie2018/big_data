package chap07;

import common.PathUtil;
import org.junit.Test;

public class FindAssociationRulesTest {
    @Test
    public void test() {
        FindAssociationRules.main(new String[]{
                PathUtil.getInputPath("sample_input_shoppingcat.txt"),
                PathUtil.getOutputPath("tmp")
        });
    }
}
