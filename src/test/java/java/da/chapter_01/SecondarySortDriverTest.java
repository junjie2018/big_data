package java.da.chapter_01;


import da.chapter_01.SecondarySortDriver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.da.MiniDFSClusterTest;


public class SecondarySortDriverTest extends MiniDFSClusterTest {
    @Test
    public void testSecondarySortDriver() throws Exception {
        String[] args = new String[]{"abc", "def"};
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
    }
}
