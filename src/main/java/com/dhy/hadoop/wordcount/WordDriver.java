package com.dhy.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/14 09:28
 */
public class WordDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.
        job.setJarByClass(WordDriver.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        //3.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //4.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5.
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //6.
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
