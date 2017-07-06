package com.dcy.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by 11019 on 17.7.6.
 */
public class TestWordCountJob {
    public static void main(String[] args) {
        try {
            Configuration entries = new Configuration();
            FileSystem fileSystem = FileSystem.get(entries);
            //获得当前mapreduce程序对应的job对象
            Job job = Job.getInstance(entries);
            //设置mapreduce程序的入口类
            job.setJarByClass(TestWordCountJob.class);
            //设置mapreduce程序输入数据源对应的文件类型
            job.setInputFormatClass(TextInputFormat.class);
            //设置mapreduce程序 输出数据源对应的文件类型
            job.setOutputFormatClass(TextOutputFormat.class);

            Path path = new Path("/src1/a.txt");
            //设置数据输入路径
            TextInputFormat.addInputPath(job,path);
            //指定计算结果的输出路径(为文件夹，输出路径必须不存在)
            Path path1 = new Path("/result");
            boolean b = fileSystem.exists(path1);
            if(b){
                fileSystem.delete(path1,true);
            }
            TextOutputFormat.setOutputPath(job,path1);

            //设置map函数所在的类
            job.setMapperClass(MyWordCountMapper.class);
            //设置reduce函数所在的类
            job.setReducerClass(MyWordCountReduce.class);

            //设置map端输出的key的类型
            job.setMapOutputKeyClass(Text.class);
            //设置map端输出的value的类型
            job.setMapOutputValueClass(IntWritable.class);
            //设置reduce端输入的key的类型
            job.setOutputKeyClass(Text.class);
            //设置reduce端输入的value的类型
            job.setOutputValueClass(IntWritable.class);

            //以同步请求的方式等待mapreduce的程序执行结果
            job.waitForCompletion(true);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
