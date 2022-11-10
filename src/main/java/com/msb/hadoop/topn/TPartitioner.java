package com.msb.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<TKey, IntWritable> {
    @Override
    public int getPartition(TKey key, IntWritable value, int numPartitions) {
        // 1.不能太复杂
        // 简单根据年份来分区，会导致数据倾斜问题
        return key.getYear() % numPartitions;
    }
}
