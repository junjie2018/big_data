package dg.chapter5.writable;

import org.apache.hadoop.io.BytesWritable;

import dg.util.WritableUtil;
import java.io.IOException;

public class BytesWritableTest {
    public void test() throws IOException {
        BytesWritable b = new BytesWritable(new byte[]{3, 5});
        byte[] bytes = WritableUtil.serialize(b);
//        assertThat
    }
}
