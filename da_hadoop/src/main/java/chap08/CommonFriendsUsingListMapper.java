package chap08;


import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendsUsingListMapper
        extends Mapper<LongWritable, Text, Text, ArrayListOfLongsWritable> {
    private static final Text REDUCER_KEY = new Text();

    static ArrayListOfLongsWritable getFriends(String[] tokens) {
        if (tokens.length == 1) {
            return new ArrayListOfLongsWritable();
        }

        ArrayListOfLongsWritable list = new ArrayListOfLongsWritable();
        for (int i = 1; i < tokens.length; i++) {
            list.add(Long.parseLong(tokens[i]));
        }
        return list;
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");
        ArrayListOfLongsWritable friends = getFriends(tokens);
        String person = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            String friend = tokens[i];
            String reduceKeyAsString = buildSortedKey(person, friend);
            REDUCER_KEY.set(reduceKeyAsString);
            context.write(REDUCER_KEY, friends);
        }
    }
}
