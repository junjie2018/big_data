package java.dg.chapter5.writable;

import dg.chapter5.other.BinaryOrTextWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GenericWritableTest {
    @Test
    @SuppressWarnings("deprecation")
    public void test() throws IOException {
        BinaryOrTextWritable src = new BinaryOrTextWritable();
        BinaryOrTextWritable dest = new BinaryOrTextWritable();

        src.set(new Text("text"));
        WritableUtils.cloneInto(dest, src);
        assertThat(dest.get(), is(new Text("text")));

        src.set(new BytesWritable(new byte[]{3, 5}));
        WritableUtils.cloneInto(dest, src);
        assertThat(((BytesWritable) dest.get()).getLength(), is(2));
    }
}
