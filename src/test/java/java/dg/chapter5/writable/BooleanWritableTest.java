package java.dg.chapter5.writable;

import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

import java.dg.util.WritableUtil;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BooleanWritableTest {
    @Test
    public void test() throws IOException {
        BooleanWritable src = new BooleanWritable(true);
        BooleanWritable dest = new BooleanWritable();
        assertThat(WritableUtil.writeTo(src, dest), is("01"));
        assertThat(dest.get(), is(src.get()));
    }
}
