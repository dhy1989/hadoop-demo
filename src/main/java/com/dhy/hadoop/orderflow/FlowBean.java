package com.dhy.hadoop.orderflow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/15 11:40
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private int upFLow;
    private int downFlow;
    private int sumFlow;

    public FlowBean() {
        super();
    }

    @Override
    public int compareTo(FlowBean o) {
        return this.sumFlow >= o.sumFlow ? -1 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFLow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFLow = dataInput.readInt();
        downFlow = dataInput.readInt();
        sumFlow = dataInput.readInt();
    }

    public int getUpFLow() {
        return upFLow;
    }

    public void setUpFLow(int upFLow) {
        this.upFLow = upFLow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(int sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFLow+"\t"+downFlow+"\t"+sumFlow;
    }
}
