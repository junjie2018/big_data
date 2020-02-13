package util;

import org.apache.hadoop.fs.Path;

import java.io.File;

public class PathUtil {

    public static final String PROJECT_ROOT = "src/test/java/resources/";

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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static Path getOutputPath(String path) {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        return new Path(file.getPath());
    }
}
