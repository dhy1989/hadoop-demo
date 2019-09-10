package com.dhy.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/14 09:24
 */
public class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable v=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        v.set(count);
        context.write(key,v);
    }
}
