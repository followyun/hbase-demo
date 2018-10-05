package com.my.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 使用java代码从hbase中读取数据
 */
public class GetTest {
    public static Configuration conf = HBaseConfiguration.create();
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
        singleGetTest();
    }

    /**
     * 获取一条数据
     */
    public static void singleGetTest() throws IOException {
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始获取一条数据 table：" + tableName.getNameAsString());
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("new_row_1"));
        //get.setMaxVersions(3)  //指定获取的版本数量为3，不设置则获取最新版本数据
        get.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null);
        get.setTimeStamp(1535811571089L);
        Result result = table.get(get);

        List<Cell> cells = result.listCells();
        for (Cell cell :
                cells) {
            System.out.println("row: " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        System.out.println("获取一条数据成功！");
    }


}
