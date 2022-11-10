package com.msb.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {

    Text rKey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TKey key, Iterable<IntWritable> values, Reducer<TKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 按照我们定义的分组器的标准，满足的进入同一个reduce。
        // 虽然reduce方法的参数中的key是单个变量，但是在context.nextKeyValue进行调用时，会对引用的值进行变更。
        Iterator<IntWritable> iterator = values.iterator();
        int index = 0;
        int preDay = 0;
        while(iterator.hasNext()){
            IntWritable val = iterator.next();
            if(index == 0) {
                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                rval.set(key.getTemperature());
                context.write(rKey, rval);
                index++;
                preDay = key.getDay();
            }else if(preDay != key.getDay()){
                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
                rval.set(key.getTemperature());
                context.write(rKey, rval);
                break;
            }
        }

    }
}
