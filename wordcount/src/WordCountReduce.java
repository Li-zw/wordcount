import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        /*
         * context是reduce的上下文
         * 上文
         * 下文
         */

        int total = 0;
        //对v3求和
        for(IntWritable v : v3){
            total += v.get();

        }
        //输出  k4 单词  v3 频率
        context.write(k3,new IntWritable(total));
    }
}
