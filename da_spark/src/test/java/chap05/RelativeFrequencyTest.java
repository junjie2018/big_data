package chap05;

import common.PathUtil;
import org.junit.Test;

public class RelativeFrequencyTest {
    @Test
    public void test() {
        RelativeFrequencyScala.main(new String[]{
                "2",
                PathUtil.getInputPath("sample_input_word.txt"),
                PathUtil.getOutputPath("tmp")
        });
    }

    @Test
    public void test2(){
        SparkSQLRelativeFrequency.main(new String[]{
                "2",
                PathUtil.getInputPath("sample_input_word.txt"),
                PathUtil.getOutputPath("tmp")
        });
    }
}
