package chap06.secondarysort;

import chap02.NaturalValue;
import chap06.TimeSeriesData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Constant;
import util.DateUtil;

import java.io.IOException;

public class SortByMRF_MovingAverageReducer
        extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    int windowSize = 5;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.windowSize = context.getConfiguration().getInt(Constant.CHAP06_WINDOW_SIZE, 5);
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {
        Text outputKey = new Text();
        Text outputValue = new Text();
        MovingAverage ma = new MovingAverage(this.windowSize);

        for (TimeSeriesData data : values) {
            ma.addNewNumber(data.getValue());

            double movingAverage = ma.getMovingAverage();
            long timestamp = data.getTimestamp();
            String dateAsString = DateUtil.getDateAsString(timestamp);

            outputKey.set(key.getName());
            outputValue.set(dateAsString + ", " + movingAverage);
            context.write(outputKey, outputValue);
        }

    }
}
