package common;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

public class PathUtil {
    private static final String TEST_ROOT = "src/test/resources/";

    public static String[] getPath(String inputFile, String outputDir) {
        if (StringUtils.isNotBlank(outputDir)) {
            String outputPath = TEST_ROOT + "output/" + outputDir;
            deleteFile(new File(outputPath));
        }

        return new String[]{
                TEST_ROOT + "input/" + inputFile,
                TEST_ROOT + "output/" + outputDir};
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] filesInFile = file.listFiles();
            if (filesInFile != null) {
                for (File fileInFile : filesInFile) {
                    deleteFile(fileInFile);
                }
            }
        }

        file.delete();
    }
}
