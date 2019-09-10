package com.dhy.hadoop.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author dinghy
 * @date 2019/8/15 10:20
 */
public class AreaPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String word = text.toString();
        String phonePrefix = word.substring(0, 3);
        return (phonePrefix.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
