package com.my.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 删除hbase数据测试
 */
public class DeleteTest {
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

        singleDeleteTest();
    }

    /**
     * 删除一条数据测试
     */
    public static void singleDeleteTest() throws IOException {
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始删除一行数据 table："+tableName.getNameAsString());
        Table table = connection.getTable(tableName);
        //删除一行数据
        Delete deleteRow = new Delete(Bytes.toBytes("new_row"));
        table.delete(deleteRow);
        System.out.println("删除一条数据成功！");
        //删除一行中的某个column family
        System.out.println("开始删除一行中的某个column family, table："+tableName.getNameAsString());
        Delete deleteColumnFamily = new Delete(Bytes.toBytes("new_row_0"));
        deleteColumnFamily.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null);
        table.delete(deleteColumnFamily);
        System.out.println("删除一行中的某个column family成功！");
    }

}
