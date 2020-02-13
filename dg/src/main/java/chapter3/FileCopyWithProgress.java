package chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;


public class FileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        String localSrc = args[0];
        String dst = args[1];

        Configuration conf = new Configuration();
        java.io.FileSystem fs = FileSystem.get(URI.create(dst), conf);

        try (InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
             OutputStream out = fs.create(new Path(dst), () -> {
                 System.out.println(".");
             })) {
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
