package com.msb.hbase.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * 当需要从hbase读取数据的时候，必须使用TableMapReduceUtil.initTableMapperJob()
 * 当需要写数据到hbase的时候，必须使用TableMapReduceUtil.initTableReduceJob()
 *      如果在代码逻辑进行实现的时候，不需要reduce，只是向hbase写数据，那么上面的方法必须存在
 *
 * 实现wordcount
 * 1.从hdfs读取数据
 * 2.将数据的结果存储到hbase
 */
public class WordCountRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node01,node02,node03");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance();
        job.setJarByClass(WordCountRunner.class);
        job.setJobName("WordCount");

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        TableMapReduceUtil.initTableReducerJob("wordcount", WordCountReducer.class, job, null, null, null, null, false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        TextInputFormat.addInputPath(job, new Path("/root/wordcount/"));
        job.waitForCompletion(true);
    }

}
