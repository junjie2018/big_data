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

    private static final String TEST_ROOT = "src/test/resources/";

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

    protected void runTest(Tool driver, String... args) throws Exception {
        driver.setConf(conf);

        Path input = new Path(TEST_ROOT + inputPath);
        Path output = new Path(TEST_ROOT + outputPath);
        fileSystem.delete(output, true);

        String[] paths = new String[]{input.toString(), output.toString()};

        String[] newArgs = new String[args.length + paths.length];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        System.arraycopy(paths, 0, newArgs, args.length, paths.length);

        System.out.println(Arrays.toString(newArgs));
        int exitCode = driver.run(newArgs);
        assertThat(exitCode, is(0));
    }

    protected void runTest(Tool driver) throws Exception {
        driver.setConf(conf);

        Path input = new Path(TEST_ROOT + inputPath);
        Path output = new Path(TEST_ROOT + outputPath);
        fileSystem.delete(output, true);

        String[] args = new String[]{input.toString(), output.toString()};

        System.out.println(Arrays.toString(args));
        int exitCode = driver.run(args);
        assertThat(exitCode, is(0));
    }
}
