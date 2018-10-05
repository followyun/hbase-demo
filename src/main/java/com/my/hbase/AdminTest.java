package com.my.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * 添加表或修改表
 */
public class AdminTest {

    public static Configuration conf = HBaseConfiguration.create();
    static {
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        //可以实时加载配置文件
        //config.addResource(new Path("src/main/resources/hbase-site.xml"));
        //config.addResource(new Path("src/main/resources/core-site.xml"));
    }
    /**
     * 新建表或覆盖表
     */
    public static void createOrOverwriteTable() throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin()
        ){
            HTableDescriptor table = new HTableDescriptor(HbaseContent.TABLE_NAME);
            table.addFamily(new HColumnDescriptor(HbaseContent.DEFAULT_COLUMN_FAMILY));
            //
            System.out.println("开始创建表");
            if(admin.tableExists(table.getTableName())){
                System.out.println("表已存在，先删除！");
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }

            admin.createTable(table);
            System.out.println("成功创建表："+table.getTableName().getNameAsString());
        }
    }

    /**
     * 向表中加字段或修改表属性
     */
    public static void modifyTable() throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin()
        ){
            TableName tableName = TableName.valueOf(HbaseContent.TABLE_NAME);
            if(!admin.tableExists(tableName)){
                System.out.println("table: "+tableName.getNameAsString()+"不存在！");
                System.exit(-1);
            }
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            //添加column
            HColumnDescriptor newColumn = new HColumnDescriptor("new_f");
            table.addFamily(newColumn);

            //修改column属性
            HColumnDescriptor existingColumn = new HColumnDescriptor(HbaseContent.DEFAULT_COLUMN_FAMILY);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            table.modifyFamily(existingColumn);
            System.out.println("开始修改表属性");
            admin.modifyTable(tableName, table);
            System.out.println("修改表属性成功");
        }
    }

    public static void main(String[] args) throws IOException {
      //  createOrOverwriteTable();
        modifyTable();
    }
}
