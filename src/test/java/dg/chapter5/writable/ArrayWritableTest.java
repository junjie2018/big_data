package dg.chapter5.writable;

import dg.chapter5.other.TextArrayWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ArrayWritableTest {
    @SuppressWarnings("deprecation")
    @Test
    public void test() throws IOException {
        ArrayWritable writable = new ArrayWritable(Text.class);
        writable.set(new Text[]{new Text("cat"), new Text("dog")});

        TextArrayWritable dest = new TextArrayWritable();
        WritableUtils.cloneInto(dest, writable);
        assertThat(dest.get().length, is(2));
        assertThat(dest.get()[0], is(new Text("cat")));
        assertThat(dest.get()[1], is(new Text("dog")));

        Text[] copy = (Text[]) dest.toArray();
        assertThat(copy[0], is(new Text("cat")));
        assertThat(copy[1], is(new Text("dog")));
    }
}
