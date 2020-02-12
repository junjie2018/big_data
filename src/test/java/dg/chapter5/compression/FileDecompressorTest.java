package dg.chapter5.compression;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Scanner;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class FileDecompressorTest {
    public static final String SF_FILE_GZ = "test.compression.file.gz";
    public static final String SF_FILE = "test.compression.file";
    private FileSystem fs;
    private Configuration conf;

    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws IOException {

        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }

        conf = new Configuration();
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();

        try (InputStream in = this.getClass().getResourceAsStream("/input/compression/file.gz");
             OutputStream out = fs.create(new Path(SF_FILE_GZ))) {
            // 将file.gz复制到文件系统中
            IOUtils.copyBytes(in, out, 4096, true);
        }
    }

    @Test
    public void decompressionGzippedFile() throws IOException {

        // 解压文件
        FileDecompressor.decompressToFileSystem(conf, fs, SF_FILE_GZ);

        // 对比解压后的文件
        assertThat(readFile(fs.open(new Path(SF_FILE))), is("Text\n"));
    }

    private String readFile(InputStream in) {
        return new Scanner(in).useDelimiter("\\A").next();
    }
}
