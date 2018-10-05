package com.my.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 批量操作hbase数据测试，增删改等
 */
public class BactchTest {
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
    public static void main(String[] args) throws IOException, InterruptedException {

        batchTest();
    }

    /**
     *
     */
    public static void batchTest() throws IOException, InterruptedException {
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始进行batch操作 table："+tableName.getNameAsString());
        List<Row> batchList = new ArrayList<>();

        Table table = connection.getTable(tableName);
        //删除一行数据
        Delete deleteRow = new Delete(Bytes.toBytes("new_row_2"));
        batchList.add(deleteRow);

        //添加一条记录
        Put put = new Put(Bytes.toBytes("new_row_12"));
        put.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY),Bytes.toBytes("q12"), Bytes.toBytes("new_row_12_value"));
        batchList.add(put);

        //查询一条记录
        Get get = new Get(Bytes.toBytes("new_row_8"));
        batchList.add(get);

        Object[] resutls = new Object[batchList.size()];
        Batch.Callback callback = new Batch.Callback() {
            @Override
            public void update(byte[] bytes, byte[] bytes1, Object o) {
                System.out.println("处理一行数据成功，table: "+Bytes.toString(bytes));
                System.out.println("row_key: "+ Bytes.toString(bytes1));
                System.out.println("keyvalues; " + o);
            }
        };
        table.batchCallback(batchList, resutls, callback);
        for (Object o :
                resutls) {
            System.out.println(o);
        }
        System.out.println("batch操作成功！");
    }

}
