package java.dg.util;

import org.apache.hadoop.fs.Path;

import java.io.File;

public class PathUtil {

    public static final String PROJECT_ROOT = "src/test/resources/";

    /**
     * 功能描述:
     * 〈输入相对于resource下的路径〉
     *
     * @param path 1
     * @return : org.apache.hadoop.fs.Path
     * @author : wujunjie
     */
    public static Path getPath(String path) {
        return new Path(PROJECT_ROOT + path);
    }
}
