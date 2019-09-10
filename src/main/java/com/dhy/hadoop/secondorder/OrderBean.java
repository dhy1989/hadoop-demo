package com.dhy.hadoop.secondorder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author dinghy
 * @date 2019/8/16 09:38
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private double price;

    public OrderBean() {
        super();
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (orderId > o.getOrderId()) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            result = this.price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
