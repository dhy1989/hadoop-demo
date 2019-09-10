package com.dhy.hadoop.secondorder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/16 10:31
 */
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
    private OrderBean k=new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        k.setOrderId(Integer.parseInt(words[0]));
        k.setPrice(Double.parseDouble(words[2]));
        context.write(k,NullWritable.get());
    }
}
