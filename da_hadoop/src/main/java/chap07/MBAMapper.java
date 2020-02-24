package chap07;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Combination;
import util.Constant;
import util.DateUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    private static final Text reduceKey = new Text();
    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    private int numberOfPairs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.numberOfPairs = context.getConfiguration().getInt(Constant.CHAP07_PAIRS, DEFAULT_NUMBER_OF_PAIRS);
        log.info("setup() numberOfPairs = " + numberOfPairs);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<String> items = convertItemsToList(line);
        if (items == null || items.isEmpty()) {
            return;
        }
        generateMapperOutput(numberOfPairs, items, context);
    }

    private static List<String> convertItemsToList(String line) {
        if (line == null || line.length() == 0) {
            return null;
        }
        String[] tokens = StringUtils.split(line, ",");
        if (tokens == null || tokens.length == 0) {
            return null;
        }
        List<String> items = new ArrayList<>();
        for (String token : tokens) {
            if (token != null) {
                items.add(token.trim());
            }
        }
        return items;
    }

    private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) throws IOException, InterruptedException {
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
        for (List<String> itemList : sortedCombinations) {
            reduceKey.set(itemList.toString());
            context.write(reduceKey, NUMBER_ONE);
        }
    }
}
