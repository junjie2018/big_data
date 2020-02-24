package chap08;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Text REDUCE_KEY = new Text();
    private static final Text REDUCE_VALUE = new Text();

    private String getFriends(String[] tokens) {
        if (tokens.length == 1) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
            builder.append(tokens[i]);
            if (i < tokens.length - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    private static String buildSortedKey(String person, String friend) {
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p < f) {
            return person + "," + friend;
        } else {
            return friend + "," + person;
        }
    }

    @Override
    /*
        100, 200 300 400 500 600
        200, 100 300 400
        300, 100 200 400 500
        400, 100 200 300
        500, 100 300
        600, 100
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");
        String friends = getFriends(tokens);
        REDUCE_VALUE.set(friends);                  // 200,300,400,500,600

        String person = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            String friend = tokens[i];
            String reduceKeyAsString = buildSortedKey(person, friend);
            REDUCE_KEY.set(reduceKeyAsString);
            context.write(REDUCE_KEY, REDUCE_VALUE);
        }
    }
}
