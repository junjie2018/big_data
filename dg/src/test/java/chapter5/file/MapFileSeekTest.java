package chapter5.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class MapFileSeekTest {
    private static final String MAP_URI = "test.numbers.map";
    private FileSystem fs;
    private MapFile.Reader reader;
    private WritableComparable<?> key;
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

        MapFileWriteDemo.writeToFileSystem(conf, fs, MAP_URI);

        reader = new MapFile.Reader(fs, MAP_URI, conf);
        key = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    }

    @Test
    public void get() throws Exception {
        Text value = new Text();
        reader.get(new IntWritable(496), value);
        assertThat(value.toString(), is("One, two, buckle my shoe"));
    }

    @Test
    public void seek() throws IOException {
        assertThat(reader.seek(new IntWritable(496)), is(true));
        assertThat(reader.next(key, value), is(true));
        assertThat(((IntWritable) key).get(), is(497));
        assertThat(value.toString(), is("Three, four, shut the door"));
    }
}
