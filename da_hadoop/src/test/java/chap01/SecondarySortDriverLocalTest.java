package chap01;

import common.LocalTest;
import org.junit.Test;


public class SecondarySortDriverLocalTest extends LocalTest {
    @Test
    public void testSecondarySortDriver() throws Exception {

        inputPath = "input/sample_input.txt";
        outputPath = "output/tmp";

        runTest(new SecondarySortDriver());
    }
}
