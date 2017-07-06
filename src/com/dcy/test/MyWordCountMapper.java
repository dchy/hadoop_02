package com.dcy.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 11019 on 17.7.6.
 */
public class MyWordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //代表hdfs中/src/a.txt
        String s = value.toString();
        String[] split = s.split(" ");
        for(String temp:split){
            context.write(new Text(temp),new IntWritable(1));

        }
    }
}
