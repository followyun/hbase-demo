package com.my.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 异步操作hbase
 */
public class BufferedMutatorTest {
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

    public static void main(String[] args) throws IOException, InterruptedException {

        bufferedMutatorTest();
    }

    public static void bufferedMutatorTest() throws IOException, InterruptedException {
        TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
        System.out.println("开始进行batch操作 table：" + tableName.getNameAsString());
        List<Mutation> batchList = new ArrayList<>();

        //删除一行数据
        Delete deleteRow = new Delete(Bytes.toBytes("new_row_3"));
        batchList.add(deleteRow);

        //添加一条记录
        Put put = new Put(Bytes.toBytes("new_row_4"));
        put.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), Bytes.toBytes("q13"), Bytes.toBytes("new_row_4_q13_value"));
        batchList.add(put);

        BufferedMutator mutator = connection.getBufferedMutator(tableName);

        mutator.mutate(batchList);
        mutator.flush();
    }
}
