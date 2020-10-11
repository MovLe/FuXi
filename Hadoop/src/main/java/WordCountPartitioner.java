import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName WordCountPartitioner
 * @MethodDesc: TODO WordCountPartitioner功能介绍
 * @Author Movle
 * @Date 10/6/20 11:32 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String firWord = key.toString().substring(0,1);

        char[] charArray = firWord.toCharArray();

        int result = charArray[0];

        if(result % 2 == 0 ){
            return 0;
        }else {
            return 1;
        }
    }
}
