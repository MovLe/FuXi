import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WordCountReducer
 * @MethodDesc: TODO WordCountReducer功能介绍
 * @Author Movle
 * @Date 10/6/20 10:57 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable value:values){
            count+=value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
