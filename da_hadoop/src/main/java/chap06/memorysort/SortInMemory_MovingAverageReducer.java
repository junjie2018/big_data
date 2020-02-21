package chap06.memorysort;

import chap06.TimeSeriesData;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Constant;
import util.DateUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class SortInMemory_MovingAverageReducer extends Reducer<Text, TimeSeriesData, Text, Text> {

    private int windowSize = 5;

    public void setup(Context context) {
        this.windowSize = context.getConfiguration().getInt(Constant.CHAP06_WINDOW_SIZE, 5);
        log.info("setup(): key = " + windowSize);
    }

    @Override
    protected void reduce(Text key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {
        log.info("reduce(): key = " + key.toString());

        List<TimeSeriesData> timeSeries = new ArrayList<>();
        for (TimeSeriesData tsData : values) {
            TimeSeriesData copy = TimeSeriesData.copy(tsData);
            timeSeries.add(copy);
        }

        // 后面的逻辑就比较简单了，就是在一个有序的数组上求移动平均
        Collections.sort(timeSeries);


        log.info("reduce(): timeSeries = {}", timeSeries.toString());

        double sum = 0.0;
        for (int i = 0; i < windowSize - 1; i++) {
            sum += timeSeries.get(i).getValue();
        }

        Text outputValue = new Text();
        for (int i = windowSize - 1; i < timeSeries.size(); i++) {
            log.info("reduce(): key = {} i = {}", key.toString(), i);
            sum += timeSeries.get(i).getValue();
            double movingAverage = sum / windowSize;
            long timestamp = timeSeries.get(i).getTimestamp();
            outputValue.set(DateUtil.getDateAsString(timestamp) + ", " + movingAverage);
            context.write(key, outputValue);

            sum -= timeSeries.get(i - windowSize + 1).getValue();
        }
    }
}
