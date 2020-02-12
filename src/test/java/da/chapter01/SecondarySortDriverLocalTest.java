package da.chapter01;


import da.LocalTest;
import org.junit.Test;


public class SecondarySortDriverLocalTest extends LocalTest {
    @Test
    public void testSecondarySortDriver() throws Exception {

        inputPath = "da/input/sample_input.txt";
        outputPath = "C:\\Users\\Junjie\\Desktop\\data\\tmp";

        runTest(new SecondarySortDriver());
    }
}
