package com.dhy.hadoop.orderflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/15 11:44
 */
public class OrderFlowMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
    private FlowBean k=new FlowBean();
    private Text v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        v.set(words[1]);
        int length = words.length;
        k.setUpFLow(Integer.parseInt(words[length-3]));
        k.setDownFlow(Integer.parseInt(words[length-2]));
        k.setSumFlow(k.getUpFLow()+k.getDownFlow());
        context.write(k,v);
    }
}
