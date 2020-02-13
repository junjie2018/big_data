package chapter5.writable;

import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

import java.io.IOException;

import static util.WritableUtil.serializeToString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class VLongWritableTest {
    @Test
    public void test() throws IOException {
        assertThat(WritableUtil.serializeToString(new VLongWritable(1)), is("01")); // 1 byte
        assertThat(WritableUtil.serializeToString(new VLongWritable(127)), is("7f")); // 1 byte
        assertThat(WritableUtil.serializeToString(new VLongWritable(128)), is("8f80")); // 2 byte
        assertThat(WritableUtil.serializeToString(new VLongWritable(163)), is("8fa3")); // 2 byte
        assertThat(WritableUtil.serializeToString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
        assertThat(WritableUtil.serializeToString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
    }
}
