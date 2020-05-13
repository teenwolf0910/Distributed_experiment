package split;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author ljw
 * @create 2020/5/11 10:22
 */
public class SplitDriver  {
    public static void main(String[] args) throws   IllegalAccessException, IOException,ClassNotFoundException,InterruptedException {
        args =new String[]{"F:\\Distributed_experiment\\Hadoop_MR_Segmentation\\input\\split.txt","F:\\Distributed_experiment\\Hadoop_MR_Segmentation\\output"};
        //获取配置信息，或者job对象实例
        Configuration configuration =new Configuration();
        Job job=Job.getInstance(configuration);
        //指定本程序jar包所在的本地路径
        job.setJarByClass(SplitDriver.class);
        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SplitReducer.class);
        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SplitBean.class);
        //指定最终输出读数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
