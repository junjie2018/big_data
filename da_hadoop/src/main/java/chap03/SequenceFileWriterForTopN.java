package chap03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

/*
    用来制造测试数据的
 */
public class SequenceFileWriterForTopN {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IOException("usage: SequenceFileWriterForTopN <hdfs-path> <number-of-entries>");
        }

        Random randomNumberGenerator = new Random();
        final String uri = args[0];
        final int N = Integer.parseInt(args[1]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        Text key = new Text();
        IntWritable value = new IntWritable();
        try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass())) {
            for (int i = 0; i < N; i++) {
                int randomInt = randomNumberGenerator.nextInt(100);
                key.set("cat" + i);
                value.set(randomInt);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
        }
    }
}
