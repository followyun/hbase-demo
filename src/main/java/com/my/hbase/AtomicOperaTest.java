package com.my.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 原子性操作测试,
 */
public class AtomicOperaTest {
    public static Configuration conf = HBaseConfiguration.create();
    public static Connection connection;
    public static Admin admin;
    public static Table table;

    static {
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        //可以实时加载配置文件
        //config.addResource(new Path("src/main/resources/hbase-site.xml"));
        //config.addResource(new Path("src/main/resources/core-site.xml"));
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            table = connection.getTable(TableName.valueOf(HbaseContent.TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("初始化hbase connection失败！");
        }
    }

    public static void main(String[] args) throws IOException {
        atomicDeleteTest();
    }

    /**
     * 原子性递增，可用作计数器，只能对有修饰符的column进行计数
     *
     */
    public static void atomicIncrementTest() throws IOException {
        Increment increment = new Increment(Bytes.toBytes("new_row_5"));
        increment.addColumn(Bytes.toBytes("user_num"), Bytes.toBytes("inc"), 2);
        table.increment(increment);
    }

    /**
     * 原子性向column的value追加内容
     *
     * @throws IOException
     */
    public static void atomicAppendColumnTest() throws IOException {
        Append append = new Append(Bytes.toBytes("new_row_13"));
        append.add(Bytes.toBytes("user_num"), Bytes.toBytes("in"), Bytes.toBytes("append_column_test"));
        table.append(append);
    }

    /**
     * 原子性put
     */
    public static void atomicPutTest() throws IOException {

        //增加操作
        Put put = new Put(Bytes.toBytes("new_row_3"));
        put.addColumn(Bytes.toBytes("new_f"), null, Bytes.toBytes("new_row_value"));

       boolean resultFlag = table.checkAndPut(Bytes.toBytes("new_row_3"), Bytes.toBytes("new_f"), null,null, put);
        System.out.println("resultFlag: "+ resultFlag);
    }

    /**
     * 原子性delete
     */
    public static void atomicDeleteTest() throws IOException {

        //删除操作
        Delete deleteRow = new Delete(Bytes.toBytes("new_row_0"));
        deleteRow.addColumn(Bytes.toBytes("new_f"), null);

        Boolean resultFlag = table.checkAndDelete(Bytes.toBytes("new_row_0"), Bytes.toBytes("new_f"),  null,null, deleteRow);
        System.out.println("resultFlag: "+ resultFlag);

    }

    /**
     * batch操作保证原子性
     * 出现异常：
     *exception in thread "main" java.lang.NullPointerException
     */
    public static void atomicBatchOperaTest() throws IOException {
        //删除操作
        Delete deleteRow = new Delete(Bytes.toBytes("new_row_5"));
        deleteRow.addColumn(Bytes.toBytes("user_num"), Bytes.toBytes("in"));

        //增加操作
        Put put = new Put(Bytes.toBytes("new_row_3"));
        put.addColumn(Bytes.toBytes(HbaseContent.DEFAULT_COLUMN_FAMILY), null, Bytes.toBytes("new_row_value"));

        RowMutations rowMutations = new RowMutations();
        rowMutations.add(deleteRow);
        rowMutations.add(put);

        table.mutateRow(rowMutations);
    }
}
