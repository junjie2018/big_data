package chap04;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
    private Text productId = new Text();
    private Text locationId = new Text("undefined");

    @Override
    /*
        1.两个mapper结果都将发送到这个reducer：
            LeftJoinUserMapper:
                [("user_id_1","1"),("L","location_id_1")]
                [("user_id_2","1"),("L","location_id_2")],
                [("user_id_3","1"),("L","location_id_3")],
                [("user_id_4","1"),("L","location_id_4")]
            LeftJoinTransactionMapper:
                [("user_id_1","2"),("P","product_id_1")],
                [("user_id_2","2"),("P","product_id_2")],
                [("user_id_3","2"),("P","product_id_3")],
                [("user_id_4","2"),("P","product_id_4")],
        2.到达这个reducer之前是可以去报按照user_id_1排好序了
     */
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {

        // 我对这个reducer的逻辑还是表示怀疑
        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()) {
            PairOfStrings firstPair = iterator.next();
            if (firstPair.getLeftElement().equals("L")) {
                locationId.set(firstPair.getRightElement());
            }
        }

        while (iterator.hasNext()) {
            PairOfStrings productPair = iterator.next();
            productId.set(productPair.getRightElement());
            context.write(productId, locationId);
        }
    }
}
