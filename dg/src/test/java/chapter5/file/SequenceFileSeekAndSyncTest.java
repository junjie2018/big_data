package chapter5.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SequenceFileSeekAndSyncTest {
    private static final String SF_URI = "test.numbers.seq";
    private FileSystem fs;
    private SequenceFile.Reader reader;
    private Writable key;
    private Writable value;

    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws IOException {

        if (null == System.getProperty("test.build.data")) {
            System.setProperty("test.build.data", "/tmp");
        }

        Configuration conf = new Configuration();
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();

        SequenceFileWritableDemo.writeToFileSystem(conf, fs, SF_URI);

        reader = new SequenceFile.Reader(fs, new Path(SF_URI), conf);
        key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    }

    @Test
    public void seekToRecordBoundary() throws IOException {
        reader.seek(359);
        assertThat(reader.next(key, value), is(true));
        assertThat(((IntWritable) key).get(), is(95));
    }

    @Test
    public void seekToNonRecordBoundary() throws IOException {
        reader.seek(360);
        reader.next(key, value);
    }

    @Test
    public void syncFromNonRecordBoundary() throws IOException {
        reader.sync(360);
        assertThat(reader.getPosition(), is(2021L));
        assertThat(reader.next(key, value), is(true));
        assertThat(((IntWritable) key).get(), is(59));
    }

    @Test
    public void syncAfterLasySyncPoint() throws IOException {
        reader.sync(4557);
        assertThat(reader.getPosition(), is(4788L));
        assertThat(reader.next(key, value), is(false));
    }
}
