package com.dcy.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by 11019 on 17.7.6.
 */
public class MyWordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            IntWritable intWritable = iterator.next();
            int i = intWritable.get();

            count+=i;
//            123
        }
        context.write(key,new IntWritable(count));
    }
}
