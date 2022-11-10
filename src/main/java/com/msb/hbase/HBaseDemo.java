package com.msb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseDemo {

    Configuration conf;
    Connection conn;
    Table table;
    Admin admin;
    TableName tableName = TableName.valueOf("full");

    @Before
    public void init() throws IOException {
        // 创建配置文件对象
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        // 加载zookeeper的配置
        conn = ConnectionFactory.createConnection(conf);
        // 获取管理对象
        admin = conn.getAdmin();
        // 获取数据操作对象
        table = conn.getTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        // 定义表描述对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        // 定义列族描述对象
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
        // 添加列族信息给表
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        // 建表
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(tableDescriptorBuilder.build());
    }

    @Test
    public void insert() throws IOException {
        Put put = new Put(Bytes.toBytes("111"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("12"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        table.put(put);
    }

    @Test
    public void get() throws IOException {
        Get get = new Get(Bytes.toBytes("111"));
        // 在服务端做数据过滤，挑选出符合需求的列
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        Result result = table.get(get);
        Cell cell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        byte[] bytes = CellUtil.cloneValue(cell);
        String name = Bytes.toString(bytes);
        System.out.println(name);
    }

    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result res : scanner){
            Cell cell = res.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("name"));
            byte[] bytes = CellUtil.cloneValue(cell);
            String name = Bytes.toString(bytes);
            System.out.println(name);
        }
    }

    @Test
    public void scanByCondition(){
        Scan scan = new Scan();
//        scan.withStartRow();
//        scan.withStopRow()
    }

    @Test
    public void getType() throws IOException {
        Scan scan = new Scan();
        // 创建过滤器集合
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 创建过滤器
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes("1"));
        filterList.addFilter(filter1);
        // 前缀过滤器
        PrefixFilter filter2 = new PrefixFilter(Bytes.toBytes("18883285496"));
        filterList.addFilter(filter2);
        scan.setFilter(filterList);
        ResultScanner rss = table.getScanner(scan);
    }

    @After
    public void destory(){
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
