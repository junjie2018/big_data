package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class LocalTest {
    private Configuration conf;
    private FileSystem fileSystem = null;

    private static final String TEST_ROOT = "src/test/resources/";

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

    protected void runTest(Tool driver, String... args) throws Exception {
        String[] argsNew = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("in:")) {
                Path input = new Path(PathUtil.getInputPath(arg.substring(3)));
                argsNew[i] = input.toString();
            } else if (arg.startsWith("out:")) {
                Path output = new Path(PathUtil.getOutputPath(arg.substring(4)));
                argsNew[i] = output.toString();
            } else {
                argsNew[i] = arg;
            }
        }

        driver.setConf(conf);

        System.out.println(Arrays.toString(argsNew));
        int exitCode = driver.run(argsNew);
        assertThat(exitCode, is(0));
    }
}
