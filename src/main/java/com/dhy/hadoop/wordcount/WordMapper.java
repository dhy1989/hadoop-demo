package com.dhy.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/14 08:48
 */
public class WordMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Text k=new Text();
    private IntWritable v=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
