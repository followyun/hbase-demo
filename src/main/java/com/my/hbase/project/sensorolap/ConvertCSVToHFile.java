package com.my.hbase.project.sensorolap;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 将保存传感器数据的CSV文件转换为Hbase支持的HFile格式文件
 * hadoop中提交应用(先启动hadoop，yarn，zookeeper，hbase服务)
 * hadoop jar ~/mr-course/hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.my.hbase.project.sensorolap.ConvertCSVToHFile \
 * hdfs://master:9999//user/bd-cqr/hbase-course/20180904/omneo.csv \
 * hdfs://master:9999//user/bd-cqr/hbase-course/hfiles/20180904 \
 * sensor
 *
 * 将HFile导入到Hbase
 * hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/bd-cqr/hbase-course/hfiles/20180904 sensor
 */
public class ConvertCSVToHFile {
    public static class ConvertCSVToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {
        public static final byte[] CF = Bytes.toBytes("v");
        public static final EncoderFactory encoderFactory = EncoderFactory.get();
        public static final ByteArrayOutputStream out = new ByteArrayOutputStream();
        public static final DatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
        public static final BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
        public static final Event event = new Event();
        public static final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1. 按都好切割csv中一行sensor数据
            //ef89,896d2423-4e1d-4402-8c24-bbbb93a049bd,WARNING,NE-435,0-0000-000,1,RQQNJBTKBYVEZBEEPAPDGOTWTYICHEMWZUHWHNJDZXZMSZCGLBWKLCHWGKMBOGNDNZSFABIMIEJVMYTIFBIFFOGGRAODZIEAWWUFRJBSN ef89
            String[] fields = value.toString().split(",");
            event.setId(fields[0]);
            event.setEventId(fields[1]);
            event.setEventType(fields[2]);
            event.setPartName(fields[3]);
            event.setPartNumber(fields[4]);
            event.setVersion(Long.parseLong(fields[5]));
            event.setPayload(fields[6]);

            //2. 序列化avro对象为 byte array
            out.reset();
            writer.write(event, encoder);
            encoder.flush();

            //3. 设置rowKey，这里是将id进行了md5编码
            byte[] rowKeyBytes = DigestUtils.md5(fields[0]);
            rowKey.set(rowKeyBytes);//rowKey为maperduce输出的key
            //4. 构造HFile中的KeyValue对象
            //rowkey("id md5"), column family("v") , qualifier(eventId),
            KeyValue keyValue = new KeyValue(rowKeyBytes, CF, Bytes.toBytes(fields[1]), out.toByteArray());
            //5. 将rowkey和keyvalue写入到HFile中去
            context.write(rowKey, keyValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.out.println("usage：<inputCsvPath> <outputHFilePath> <tableName>");
            System.exit(-1);
        }
        String inputCsvPath = args[0]; //输入的csv目录
        String outputHFilePath = args[1];//输出的HFile目录
        TableName tableName = TableName.valueOf(args[2]);//Hbase中的表名

        //初始化Hbase环境
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(tableName);

        //初始化mapreduce环境
        Job job = Job.getInstance(conf, "ConvertCSVToHFile");
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));//输出格式为hbase

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, inputCsvPath);//设置输入目录

        job.setJarByClass(ConvertCSVToHFile.class);
        job.setMapperClass(ConvertCSVToHFileMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        HFileOutputFormat2.setOutputPath(job, new Path(outputHFilePath));//设置输出目录
        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(-1);
        }

    }
}
