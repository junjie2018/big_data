package da.chapter01;


import da.chapter01.mr.SecondarySortDriver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import da.MiniDFSClusterTest;


public class SecondarySortDriverTest extends MiniDFSClusterTest {
    @Test
    public void testSecondarySortDriver() throws Exception {
        String[] args = new String[]{"abc", "def"};
        ToolRunner.run(new SecondarySortDriver(), args);
    }
}
