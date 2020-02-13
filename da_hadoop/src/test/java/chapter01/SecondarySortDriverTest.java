package chapter01;

import common.MiniDFSClusterTest;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class SecondarySortDriverTest extends MiniDFSClusterTest {
    @Test
    public void testSecondarySortDriver() throws Exception {
        String[] args = new String[]{"abc", "def"};
        ToolRunner.run(new SecondarySortDriver(), args);
    }
}
