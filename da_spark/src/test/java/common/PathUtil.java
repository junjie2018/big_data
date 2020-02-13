package common;

public class PathUtil {
    private static final String TEST_ROOT = "src/test/resources/";

    public static String[] getPath(String inputFile, String outputDir) {
        return new String[]{
                TEST_ROOT + "input/" + inputFile,
                TEST_ROOT + "output/" + outputDir};
    }
}
