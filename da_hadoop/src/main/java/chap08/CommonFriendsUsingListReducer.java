package chap08;

import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class CommonFriendsUsingListReducer extends Reducer<Text, ArrayListOfLongsWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<ArrayListOfLongsWritable> values, Context context) throws IOException, InterruptedException {
        Map<Long, Integer> map = new HashMap<>();
        Iterator<ArrayListOfLongsWritable> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            ArrayListOfLongsWritable friends = iterator.next();
            if (friends == null) {
                context.write(key, null);
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        List<Long> commonFriends = new ArrayList<>();
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }

        context.write(key, new Text(commonFriends.toString()));
    }

    private void addFriends(Map<Long, Integer> map, ArrayListOfLongsWritable friendsList) {
        for (long id : friendsList) {
            map.put(id, map.getOrDefault(id, 0) + 1);
        }
    }
}
