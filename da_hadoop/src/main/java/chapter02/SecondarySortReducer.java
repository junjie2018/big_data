package chapter02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.DateUtil;

import java.io.IOException;

public class SecondarySortReducer
        extends Reducer<CompositeKey, NaturalValue, Text, Text> {

    @Override
    protected void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (NaturalValue data : values) {
            String dateAsString = DateUtil.getDateAsString(data.getTimestamp());
            double price = data.getPrice();
            builder.append("(").append(dateAsString).append(",").append(price).append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    }
}
