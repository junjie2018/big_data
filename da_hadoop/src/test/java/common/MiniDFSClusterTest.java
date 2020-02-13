package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class MiniDFSClusterTest {
    protected MiniDFSCluster cluster;
    protected FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }

        Configuration configuration = new Configuration();
        cluster = new MiniDFSCluster.Builder(configuration).build();
        fileSystem = cluster.getFileSystem();
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
}
