package com.msb.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    // hadoop框架中，是一个分布式数据，涉及序列化、反序列化
    // 也可以自己实现序列化，反序列化接口，以及比较器接口
    // 需要排序。有两种顺序：字典序、数值顺序
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();



    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}
