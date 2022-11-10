package com.msb.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {

    // 因为map可能被多次吊起，定义在外边减少gc。源码中map输出的key，value会序列化为字节数组进入buffer
    TKey mkey = new TKey();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TKey, IntWritable>.Context context) throws IOException, InterruptedException {
        // 数据样例：2019-6-1 22:22:22   1   31
        String[] splits = StringUtils.split(value.toString(), '\t');
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = sdf.parse(splits[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            mkey.setYear(calendar.get(Calendar.YEAR));
            mkey.setMonth(calendar.get(Calendar.MONTH)-1);
            mkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            mkey.setTemperature(Integer.parseInt(splits[2]));
            mval.set(Integer.parseInt(splits[2]));
            context.write(mkey, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
