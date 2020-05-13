package split;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author ljw
 * @create 2020/5/11 9:52
 */
public class SplitReducer extends Reducer<IntWritable,SplitBean, NullWritable, Text> {
    Text v=new Text();
    @Override
    protected  void reduce(IntWritable key,Iterable<SplitBean>values,Context context) throws IOException,InterruptedException{

        float a=0;
        float b=0;
        int  n=0;
        for (SplitBean value : values) {
            a=Float.parseFloat(value.getA());
            b=Float.parseFloat(value.getB());
            n=Integer.parseInt(value.getN());
        }
        float num[]=new float[n];
        float flag=(b-a)/(n+1);
        for(int i=0;i<n;i++){
            num[i]=((float)((int)(flag*(i+1)*100+0.5)))/100;
        }
        if(n==1){
            v.append(String.valueOf(num[0]).getBytes(),0,String.valueOf(num[0]).length());
            context.write(NullWritable.get(),v);
            v.clear();
        }
        else {
            for(int j=0;j<n-1;j++){
                String va=String.valueOf(num[j])+",";
                v.append(va.getBytes(),0,va.length());
            }
            v.append(String.valueOf(num[n-1]).getBytes(),0,String.valueOf(num[n-1]).length());
            context.write(NullWritable.get(),v);
            v.clear();
        }
    }

}
