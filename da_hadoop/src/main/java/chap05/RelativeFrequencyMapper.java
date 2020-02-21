package chap05;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;
    private final PairOfWords pair = new PairOfWords();
    private final IntWritable totalCount = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), " ");
        if ((tokens == null) || (tokens.length < 2)) {
            return;
        }

        for (int i = 0; i < tokens.length; i++) {
            // 替换掉当前token中的所有非单词（我给的数据，会进行避免触发该条件）
            tokens[i] = tokens[i].replaceAll("\\W+", "");
            if (tokens[i].equals("")) {
                continue;
            }

            pair.setWord(tokens[i]);

            int start = (i - neighborWindow < 0) ? 0 : i - neighborWindow;
            int end = (i + neighborWindow >= tokens.length) ? tokens.length - 1 : i + neighborWindow;
            for (int j = start; j <= end; j++) {
                if (j == i) {
                    continue;
                }

                pair.setNeighbor(tokens[j].replaceAll("\\W+", ""));
                context.write(pair, ONE);
            }

            // 统计当前word总共有多少个相邻元素
            pair.setNeighbor("*");
            totalCount.set(end - start);
            context.write(pair, totalCount);
        }
    }
}
