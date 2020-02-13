package chapter5.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import util.WritableUtil;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class IntWritableTest {
    @Test
    public void test() throws IOException {
        IntWritable src = new IntWritable(163);
        IntWritable dest = new IntWritable();
        assertThat(WritableUtil.writeTo(src, dest), is("000000a3"));
        assertThat(dest.get(), is(src.get()));
    }

    @Test
    public void walkthroughWithNoArgsConstructor() throws IOException {
        IntWritable writable = new IntWritable();
        writable.set(163);
        checkWalkthrough(writable);
    }

    @Test
    public void walkthroughWithValueConstructor() throws IOException {
        IntWritable writable = new IntWritable(163);
        checkWalkthrough(writable);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void comparator() throws IOException {
        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);

        // 对象层次的比较
        IntWritable w1 = new IntWritable(163);
        IntWritable w2 = new IntWritable(67);
        assertThat(comparator.compare(w1, w2), greaterThan(0));

        // 字节数组层次的比较
        byte[] b1 = WritableUtil.serialize(w1);
        byte[] b2 = WritableUtil.serialize(w2);
        assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length), greaterThan(0));
    }


    private void checkWalkthrough(IntWritable writable) throws IOException {
        // 先将writable序列化成字节数组
        byte[] bytes = WritableUtil.serialize(writable);

        // 判断序列化后的长度是不是4
        assertThat(bytes.length, is(4));

        // 判断系列化后的结果是不是000000a3
        assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));

        // 将bytes反序列化成Writable
        IntWritable newWritable = new IntWritable();
        WritableUtil.deserialize(newWritable, bytes);

        // 判断反序列结果是不是163
        assertThat(newWritable.get(), is(163));
    }
}
