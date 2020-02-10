package java.dg.chapter5.writable;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import java.dg.util.WritableUtil;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class NullWritableTest {
    @Test
    public void test() throws IOException {
        NullWritable writable = NullWritable.get();
        assertThat(WritableUtil.serialize(writable).length, is(0));
    }
}
