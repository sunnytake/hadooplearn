package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class TestHDFS {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void conn() throws IOException, URISyntaxException, InterruptedException {
        // 加载resource下的配置文件
        conf = new Configuration();
        // 方法1：去环境变量load HADOOP_USER_NAME
        fs = FileSystem.get(conf);
        // 方法2：指定用户名
        fs = FileSystem.get(new URI("hdfs://mycluster/"), conf, "root");

    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/msb");
        if(fs.exists(path))
            fs.delete(path);
        fs.mkdirs(path);
    }

    @Test
    public void upload() throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(new File("./hello.txt")));
        Path outFile = new Path("/msb/out.txt");
        FSDataOutputStream output = fs.create(outFile);
        IOUtils.copyBytes(bufferedInputStream, output, conf, true);
    }

    @Test
    public void blocks() throws IOException {
        Path file = new Path("/user/root/data.txt");
        FileStatus fileStatus = fs.getFileStatus(file);
        BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, fileStatus.getLen());
        for(BlockLocation b : blockLocations){
            System.out.println(b);
        }
    }

    @After
    public void close() throws IOException {
        fs.close();
    }
}
