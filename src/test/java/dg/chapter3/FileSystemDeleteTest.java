package dg.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FileSystemDeleteTest {
    private MiniDFSCluster cluster;
    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {

        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }

        Configuration configuration = new Configuration();
        cluster = new MiniDFSCluster.Builder(configuration).build();
        fileSystem = cluster.getFileSystem();

        fileSystem = FileSystem.get(configuration);
        writeFile(fileSystem, new Path("dir/file")); // todo 这儿可能会报错吧
    }

    @After
    public void tearDown() throws IOException {
        if (null != fileSystem) {
            fileSystem.close();
        }

        if (null != cluster) {
            cluster.shutdown();
        }
    }


    private void writeFile(FileSystem fileSystem, Path path) throws IOException {
        FSDataOutputStream stm = fileSystem.create(path);
        stm.write("content".getBytes(StandardCharsets.UTF_8));
        stm.close();
    }

    @Test
    public void deleteFile() throws IOException {
        assertThat(fileSystem.delete(new Path("dir/file"), false), is(true));
        assertThat(fileSystem.exists(new Path("dir/file")), is(false));
        assertThat(fileSystem.exists(new Path("dir")), is(true));
        assertThat(fileSystem.delete(new Path("dir"), false), is(true));
        assertThat(fileSystem.exists(new Path("dir")), is(false));
    }

    @Test
    public void deleteNonEmptyDirectoryNonRecursivelyFails() throws Exception {
        try {
            fileSystem.delete(new Path("dir"), false);
            fail("Shouldn't delete non-empty directory");
        } catch (Exception e) {
            // expected
//            fail("Should't delete non-empty directory");
        }
    }

    @Test
    public void deleteDirectory() throws Exception {
        assertThat(fileSystem.delete(new Path("dir"), true), is(true));
        assertThat(fileSystem.exists(new Path("dir")), is(false));
    }
}
