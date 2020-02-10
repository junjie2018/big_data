package java.dg.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CoherencyModelTest {
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

    @Test
    public void fileExistsImmediatelyAfterCreating() throws IOException {
        Path p = new Path("p");
        fileSystem.create(p);
        assertThat(fileSystem.exists(p), is(true));
        assertThat(fileSystem.delete(p, true), is(true));
    }

    @Test
    public void fileContentIsNotVisableAfterFlush() throws IOException {
        Path p = new Path("p");
        OutputStream outputStream = fileSystem.create(p);
        outputStream.write("content".getBytes(StandardCharsets.UTF_8));
        assertThat(fileSystem.exists(p), is(true));
        assertThat(fileSystem.getFileStatus(p).getLen(), is(0L));
        outputStream.close();
        assertThat(fileSystem.delete(p, true), is(true));
    }

    @Test
    public void fileContentIsVisibleAfterHFlush() throws IOException {
        Path p = new Path("p");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(p);
        fsDataOutputStream.write("content".getBytes(StandardCharsets.UTF_8));
        fsDataOutputStream.hflush();
        fsDataOutputStream.close();
        assertThat(fileSystem.delete(p, true), is(true));
    }

    @Test
    public void fileContentIsVisibleAfterHSync() throws IOException {
        Path p = new Path("p");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(p);
        fsDataOutputStream.write("content".getBytes(StandardCharsets.UTF_8));
        fsDataOutputStream.hsync();
        assertThat(fileSystem.getFileStatus(p).getLen(), is((long) "content".length()));
        fsDataOutputStream.close();
        assertThat(fileSystem.delete(p, true), is(true));
    }

    @Test
    public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
        File localFile = File.createTempFile("tmp", "");
        assertThat(localFile.exists(), is(true));
        FileOutputStream out = new FileOutputStream(localFile);
        out.write("content".getBytes(StandardCharsets.UTF_8));
        out.flush();
        out.getFD().sync();
        assertThat(localFile.length(), is((long) "content".length()));
        out.close();
        assertThat(localFile.delete(), is(true));
    }

    @Test
    public void fileContentIsVisibleAfterClose() throws IOException {
        Path p = new Path("p");
        OutputStream outputStream = fileSystem.create(p);
        outputStream.write("content".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        assertThat(fileSystem.getFileStatus(p).getLen(), is((long) "content".length()));
        assertThat(fileSystem.delete(p, true), is(true));
    }
}
