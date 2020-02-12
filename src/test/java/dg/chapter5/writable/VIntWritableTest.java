package dg.chapter5.writable;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.io.IOException;

import static dg.util.WritableUtil.serialize;
import static dg.util.WritableUtil.serializeToString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class VIntWritableTest {
    @Test
    public void test() throws IOException {
        byte[] data = serialize(new VIntWritable(163));
        assertThat(StringUtils.byteToHexString(data), is("8fa3"));
    }

    @Test
    public void testSize() throws IOException {
        assertThat(serializeToString(new VIntWritable(1)), is("01")); // 1 byte
        assertThat(serializeToString(new VIntWritable(-112)), is("90")); // 1 byte
        assertThat(serializeToString(new VIntWritable(127)), is("7f")); // 1 byte
        assertThat(serializeToString(new VIntWritable(128)), is("8f80")); // 2 byte
        assertThat(serializeToString(new VIntWritable(163)), is("8fa3")); // 2 byte
        assertThat(serializeToString(new VIntWritable(Integer.MAX_VALUE)),
                is("8c7fffffff")); // 5 byte
        assertThat(serializeToString(new VIntWritable(Integer.MIN_VALUE)),
                is("847fffffff")); // 5 byte
    }
}
