package chapter5.writable;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ObjectWritableTest {
    @Test
    @SuppressWarnings("deprecation")
    public void test() throws IOException {
        ObjectWritable src = new ObjectWritable(Integer.TYPE, 163);
        ObjectWritable dest = new ObjectWritable();
        WritableUtils.cloneInto(dest, src);
        assertThat(dest.get(), is(163));
    }
}
