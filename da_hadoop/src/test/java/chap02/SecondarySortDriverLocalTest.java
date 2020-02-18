package chap02;

import common.LocalTest;
import org.junit.Test;


public class SecondarySortDriverLocalTest extends LocalTest {
    @Test
    public void testSecondarySortDriver() throws Exception {

        inputPath = "input/sample_input2.txt";
        outputPath = "output/tmp";

        runTest(new SecondarySortDriver());
    }
}
