package com.dhy.hadoop.secondorder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author dinghy
 * @date 2019/8/16 11:09
 */
public class OrderPartitioner extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrderId() & Integer.MAX_VALUE) % numPartitions;
    }
}
