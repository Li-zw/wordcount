import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
         * context 表示Mapper的上下文
         * 上文：HDFS
         * 下文：Mapper
         */
        //数据：a good beginning is half the battle
        String data = value.toString();     //将读入的数据转为String 类型
        String [] words = data.split(" ");  //按照一定的格式进行切片并保存在数组中
        //输出 <k2,v2>  ——>  <a,1> <good,1>
        for (String str : words){
            context.write(new Text(str),new IntWritable(1));    //指定输出格式，作为Reduce的输入
       }
    }

}
