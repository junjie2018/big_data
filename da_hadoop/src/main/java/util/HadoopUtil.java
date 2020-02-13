package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopUtil {
    public static List<FileStatus> getDirectoryListing(String directory, FileSystem fs) throws IOException {
        Path dir = new Path(directory);
        FileStatus[] statuses = fs.listStatus(dir);
        return Arrays.asList(statuses);
    }

    // 不是很懂这个要干嘛
    public static void addJarsToDistributedCache(Configuration conf, String hdfsJarDirectory) throws IOException {
        if (conf == null) {
            return;
        }
        FileSystem fs = FileSystem.get(conf);
        List<FileStatus> jars = getDirectoryListing(hdfsJarDirectory, fs);
        for (FileStatus jar : jars) {
            Path jarPath = jar.getPath();
            DistributedCache.addFileToClassPath(jarPath, conf, fs);
        }
    }

    // 不是很懂这个要干嘛
    public static void addJarsToDistributedCache(Job job, String hdfsJarDirectory) throws IOException {
        if (job == null) {
            return;
        }
        addJarsToDistributedCache(job.getConfiguration(), hdfsJarDirectory);
    }

    public static boolean pathExists(Path path, FileSystem fs) {
        if (path == null) {
            return false;
        }
        try {
            return fs.exists(path);
        } catch (Exception e) {
            return false;
        }
    }

    public List<String> listDirectoryAsListOfString(String directory, FileSystem fs) throws IOException {
        Path path = new Path(directory);
        FileStatus[] statuses = fs.listStatus(path);
        List<String> listing = new ArrayList<>();
        for (FileStatus status : statuses) {
            listing.add(status.getPath().toUri().getPath());
        }
        return listing;
    }
}
