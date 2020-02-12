package dg.chapter2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import dg.util.PathUtil;
import java.io.*;
import java.util.Arrays;


public class MaxTemperatureTest {
    @Test
    public void map() throws IOException {
        Text value = new Text("xxxxxxxxxxxxxxx1950xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-00111xxxxxxxx");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void reduce() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"),
                        Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }

    @Test
    public void drive() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("mapreduce.framework.name", "local");

        Path input = PathUtil.getPath("input/ncdc/sample");
        Path output = PathUtil.getPath("output/ncdc/sample");

        FileSystem fs = FileSystem.getLocal(configuration);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(configuration);

        int exitCode = driver.run(new String[]{
                input.toString(),
                output.toString()});
        assertThat(exitCode, is(0));

        checkOutput(configuration, output);
    }

    @Test
    public void driveWithCombiner() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("mapreduce.framework.name", "local");

        Path input = PathUtil.getPath("input/ncdc/sample");
        Path output = PathUtil.getPath("output/ncdc/sample");

        FileSystem fs = FileSystem.getLocal(configuration);
        fs.delete(output, true);

        MaxTemperatureDriverWithCombiner driver = new MaxTemperatureDriverWithCombiner();
        driver.setConf(configuration);
//
        int exitCode = driver.run(new String[]{
                input.toString(),
                output.toString()});
        assertThat(exitCode, is(0));
//
        checkOutput(configuration, output);
    }

    private void checkOutput(Configuration configuration, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(configuration);

        // 列出output路径下的文件，列出时添加一个过滤器
        Path[] outputFiles = FileUtil.stat2Paths(
                fs.listStatus(output, path -> !path.getName().startsWith("_")));
        assertThat(outputFiles.length, is(1));

        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(
                new FileInputStream(PathUtil.getPath("expected/ncdc/sample/sample_expected").toString()));

        String expectedLine;
        while ((expectedLine = expected.readLine()) != null) {
            assertThat(actual.readLine(), is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());
        actual.close();
        expected.close();
    }

    private BufferedReader asBufferedReader(InputStream in) {
        return new BufferedReader(new InputStreamReader(in));
    }
}
