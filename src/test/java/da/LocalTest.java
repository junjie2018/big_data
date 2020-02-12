package da;

import da.chapter01.SecondarySortDriver;
import dg.util.PathUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class LocalTest {

    protected String inputPath = "null";
    protected String outputPath = "C:\\Users\\Junjie\\Desktop\\data\\tmp";

    protected Configuration conf;
    protected FileSystem fileSystem = null;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");


        fileSystem = FileSystem.getLocal(conf);
    }

    @After
    public void tearDown() throws IOException {
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

    protected void runTest(Tool driver) throws Exception {
        driver.setConf(conf);

        Path input = PathUtil.getPath(inputPath);
        Path output = PathUtil.getOutputPath(outputPath);

        String[] args = new String[]{input.toString(), output.toString()};

        System.out.println(Arrays.toString(args));
        int exitCode = driver.run(args);
        assertThat(exitCode, is(0));
    }
}
