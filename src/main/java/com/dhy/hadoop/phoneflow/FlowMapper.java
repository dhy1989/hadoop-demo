package com.dhy.hadoop.phoneflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/15 09:01
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text k = new Text();
    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        k.set(words[1]);
        int length = words.length;
        int upFlow = Integer.parseInt(words[length - 3]);
        int downFlow = Integer.parseInt(words[length - 2]);
        int sumFlow = upFlow + downFlow;
       v.setUpFlow(upFlow);
       v.setDownFlow(downFlow);
       v.setSumFlow(sumFlow);
       context.write(k,v);
    }
}
