package com.msb.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TKey implements WritableComparable {

    private int year;
    private int month;
    private int day;
    private int temperature;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(Object o) {
        TKey that = (TKey)o;
        // 为了单纯的多做api开发，所以类型的比较器采用通用逻辑，按照时间正序排序。
        // 但是业务需要的是年月温度，且温度倒叙，所以还需要开发一个排序比较器
        int res1 = Integer.compare(this.year, that.getYear());
        if(res1 == 0){
            int res2 = Integer.compare(this.month, that.getMonth());
            if(res2 == 0)
                return Integer.compare(this.day, that.getDay());
            return res2;
        }
        return res1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.day);
        dataOutput.writeInt(this.temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.temperature = dataInput.readInt();
    }
}
