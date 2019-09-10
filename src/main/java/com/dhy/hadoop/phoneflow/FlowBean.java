package com.dhy.hadoop.phoneflow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/15 08:46
 */
public class FlowBean implements Writable {
    private int upFlow;
    private int downFlow;
    private int sumFlow;

    public FlowBean() {
        super();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readInt();
        downFlow = dataInput.readInt();
        sumFlow = dataInput.readInt();
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
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
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
