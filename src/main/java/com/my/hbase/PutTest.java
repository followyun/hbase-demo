package com.my.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用java代码向hbase中写入数据
 */
public class PutTest {
    public static Configuration conf =HBaseConfiguration.create();
    public static Connection connection;
    public static Admin admin;
    static {
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        //可以实时加载配置文件
        //config.addResource(new Path("src/main/resources/hbase-site.xml"));
        //config.addResource(new Path("src/main/resources/core-site.xml"));
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("初始化hbase connection失败！");
        }
    }
    public static void main(String[] args) throws IOException {

        batchPutTest(10);
    }

    /**
     * 插入一条数据
     */
    public static void singlePutTest() throws IOException {
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始插入一条数据到 table："+tableName.getNameAsString());
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes("new_row"));
        put.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null, Bytes.toBytes("new_row_value"));
        table.put(put);
        System.out.println("插入一条数据成功！");
    }

    /**
     * 插入多条数据到表中
     * @param n n为要插入的数据
     * @throws IOException
     */
    public static void batchPutTest(long n) throws IOException{
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始插入多条数据到 table："+tableName.getNameAsString());
        Table table = connection.getTable(tableName);
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Put put = new Put(Bytes.toBytes("new_row_"+ i));
            put.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null, Bytes.toBytes("new_row_value_"+i));
            puts.add(put);
        }

        table.put(puts);
        System.out.println("插入多条数据成功！");
    }
}
