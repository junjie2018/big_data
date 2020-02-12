package dg.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class ShowFileStatusTest {

    private MiniDFSCluster cluster;
    private FileSystem fileSystem;

    @Before
    public void setUp() throws IOException {
        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }

        Configuration configuration = new Configuration();
        cluster = new MiniDFSCluster.Builder(configuration).build();
        fileSystem = cluster.getFileSystem();

        OutputStream out = fileSystem.create(new Path("/dir/file"));
        out.write("content".getBytes(StandardCharsets.UTF_8));
        out.close();
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

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fileSystem.getFileStatus(new Path("no-such-file"));
    }


    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        FileStatus stat = fileSystem.getFileStatus(file);

        assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));                          // getPath()
        assertThat(stat.isDirectory(), is(false));                                              // isDirectory()
        assertThat(stat.getLen(), is(7L));                                                      // getLen()
        assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));    // getModificationTime()
        assertThat(stat.getReplication(), is((short) 1));                                             // getReplication()
        assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));                                // getBlockSize()
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));                             // getOwner()
        assertThat(stat.getGroup(), is("supergroup"));                                          // getGroup()
        assertThat(stat.getPermission().toString(), is("rw-r--r--"));                           // getPermission()
    }

    @Test
    public void fileStatusForDirectory() throws IOException {
        Path dir = new Path("/dir");
        FileStatus stat = fileSystem.getFileStatus(dir);

        assertThat(stat.getPath().toUri().getPath(), is("/dir"));
        assertThat(stat.isDirectory(), is(true));
        assertThat(stat.getLen(), is(0L));
        assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(stat.getReplication(), is((short) 0));
        assertThat(stat.getBlockSize(), is(0L));
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));
        assertThat(stat.getGroup(), is("supergroup"));
        assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
    }
}
