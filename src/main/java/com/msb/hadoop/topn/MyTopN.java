package com.msb.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyTopN {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance();
        job.setJarByClass(MyTopN.class);
        job.setJobName("TopN");


        // maptask
        // input-output
        TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        Path outPath = new Path(remainingArgs[1]);
        if(outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath, true);
        TextOutputFormat.setOutputPath(job, outPath);
        // map
        job.setMapperClass(TMapper.class);
        // key-value
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        // partitioner 按年月分区，分区>分组，甚至可以按年分区。只要确保一组进入同一个分区，多个组的数据可以进同一个分区
        job.setPartitionerClass(TPartitioner.class);
        // sortComparator 年月温度，且温度倒叙
        job.setSortComparatorClass(TSortComparator.class);
        // combiner
//        job.setCombinerClass();


        // reducetask
        // groupComparator
        job.setGroupingComparatorClass(TGroupingComparator.class);
        //reduce
        job.setReducerClass(TReducer.class);


        job.waitForCompletion(true);


    }
}
