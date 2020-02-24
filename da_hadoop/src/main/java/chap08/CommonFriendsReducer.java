package chap08;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 我认为如果记录正确的话，每一用户都只有一条记录，所以这个地方最多只有两条记录
        // 但是作者用numOfValues进行了统计，更严谨，但是我需要知道在什么情况下，这个地方的值不为2
        Map<String, Integer> map = new HashMap<>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            String friends = iterator.next().toString();
            if (friends.equals("")) {
                context.write(key, new Text("[]"));
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        List<String> commonFriends = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }

        context.write(key, new Text(commonFriends.toString()));
    }

    private static void addFriends(Map<String, Integer> map, String friendList) {
        String[] friends = StringUtils.split(friendList, ",");
        for (String friend : friends) {
            map.put(friend, map.getOrDefault(friend, 0) + 1);
        }
    }
}
