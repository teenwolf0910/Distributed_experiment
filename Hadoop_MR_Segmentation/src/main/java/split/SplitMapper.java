package split;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author ljw
 * @create 2020/5/11 9:14
 */
public class SplitMapper extends Mapper<LongWritable, Text,IntWritable,SplitBean> {

    IntWritable k=new IntWritable();
    SplitBean v=new SplitBean();
    static int flag=1;
    //    static IntWritable flag=null;
    @Override
    protected  void map (LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, SplitBean>.Context context) throws IOException, InterruptedException{
        //1.获取行
        String line=value.toString();
        //2.切割
        String words[]=line.split(",");
        //3.塞值
        k.set(flag);
        v.set(words[0],words[1],words[2]);
        flag++;
        //shuchu
        context.write(k,v);
    }
}
