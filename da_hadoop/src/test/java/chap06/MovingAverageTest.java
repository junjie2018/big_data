package chap06;

import chap06.memorysort.SortInMemory_MovingAverageDriver;
import chap06.pojo.SimpleMovingAverage;
import chap06.secondarysort.SortByMRF_MovingAverageDriver;
import common.LocalTest;
import common.PathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;

@Slf4j
public class MovingAverageTest extends LocalTest {
    @Test
    public void test() {
        // 这个应该是为了避免写log4j.properties文件
        // BaseConfigurator.configure();

        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int[] allWindowSizes = {3, 4};

        for (int windowSize : allWindowSizes) {
            SimpleMovingAverage sma = new SimpleMovingAverage(windowSize);
            log.info("windowSize = " + windowSize);
            for (double x : testData) {
                sma.addNewNumber(x);
                log.info("Next number = " + x + ", SMA = " + sma.getMovingAverage());
            }
            log.info("---");
        }
    }

    @Test
    public void test2() throws Exception {
        runTest(new SortInMemory_MovingAverageDriver(),
                "3",
                "in:sample_input_average.txt",
                "out:tmp"
        );
    }

    @Test
    public void test3() throws Exception {
        runTest(new SortByMRF_MovingAverageDriver(),
                "3",
                "in:sample_input_average.txt",
                "out:tmp"
        );
    }
}
