package com.dhy.hadoop.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/15 09:27
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean v=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int sumUpFlow=0;
        int sumDownFlow=0;
        for (FlowBean value : values) {
            sumUpFlow+=value.getUpFlow();
            sumDownFlow+=value.getDownFlow();
        }
        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        v.setSumFlow(sumUpFlow+sumDownFlow);
        context.write(key,v);
    }
}
