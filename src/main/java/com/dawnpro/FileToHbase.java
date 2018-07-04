package com.dawnpro;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FileToHbase {



    public static class CoverHDFSToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
    {

        Log loger = LogFactory.getLog(FileToHbase.class);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String stanMsg = value.toString();

            String[] stanArray = stanMsg.split(",");
            loger.info("分割出来的字段个数为："+stanArray.length );
            if(stanArray.length == 27){
                String vin            = stanArray[0];
                String time           = stanArray[1];
                String commandtype    = stanArray[2];
                String vehicle_status = stanArray[3];
                String charge_state   = stanArray[4];
                String run_model      = stanArray[5];
                String speed          = stanArray[6];
                String accu_mile      = stanArray[7];
                String total_voltage  = stanArray[8];
                String total_current  = stanArray[9];
                String soc            = stanArray[10];
                String dc_dc_status   = stanArray[11];
                String gear           = stanArray[12];
                String ir             = stanArray[13];
                String location_state = stanArray[14];
                String longitude      = stanArray[15];
                String latitude       = stanArray[16];
                String etl_author     = stanArray[17];
                String last_modified  = stanArray[18];
                String hbase_rowkey = vin+time;
                byte[] rowKey=Bytes.toBytes(hbase_rowkey);
                ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
                byte[] family=Bytes.toBytes("tboxinfo");
                Put put=new Put(rowKey);
                put.add(family, Bytes.toBytes("vin"), Bytes.toBytes(vin));
                put.add(family, Bytes.toBytes("time"), Bytes.toBytes(time));
                put.add(family, Bytes.toBytes("commandtype"), Bytes.toBytes(commandtype));
                put.add(family, Bytes.toBytes("vehicle_status"), Bytes.toBytes(vehicle_status));
                put.add(family, Bytes.toBytes("charge_state"), Bytes.toBytes(charge_state));
                put.add(family, Bytes.toBytes("run_model"), Bytes.toBytes(run_model));
                put.add(family, Bytes.toBytes("speed"), Bytes.toBytes(speed));
                put.add(family, Bytes.toBytes("accu_mile"), Bytes.toBytes(accu_mile));
                put.add(family, Bytes.toBytes("total_voltage"), Bytes.toBytes(total_voltage));
                put.add(family, Bytes.toBytes("total_current"), Bytes.toBytes(total_current));
                put.add(family, Bytes.toBytes("soc"), Bytes.toBytes(soc));
                put.add(family, Bytes.toBytes("dc_dc_status"), Bytes.toBytes(dc_dc_status));
                put.add(family, Bytes.toBytes("gear"), Bytes.toBytes(gear));
                put.add(family, Bytes.toBytes("ir"), Bytes.toBytes(ir));
                put.add(family, Bytes.toBytes("location_state"), Bytes.toBytes(location_state));
                put.add(family, Bytes.toBytes("longitude"), Bytes.toBytes(longitude));
                put.add(family, Bytes.toBytes("latitude"), Bytes.toBytes(latitude));
                put.add(family, Bytes.toBytes("etl_author"), Bytes.toBytes(etl_author));
                put.add(family, Bytes.toBytes("last_modified"), Bytes.toBytes(last_modified));
                context.write(rowKeyWritable, put);
            }
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration hadoopConfiguration=new Configuration();
        String[] dfsArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();
        if (dfsArgs.length != 3) {
            System.err.println("Usage: TripTaggerMan <tablename> <input> <out>");
            System.exit(2);
        }

        Job fileToHabseJob=new Job(hadoopConfiguration, "FileToHbaseJob");
        fileToHabseJob.setJarByClass(FileToHbase.class);
        fileToHabseJob.setMapperClass(CoverHDFSToHFileMapper.class);
        fileToHabseJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
        fileToHabseJob.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(fileToHabseJob, new Path(dfsArgs[1]));
        FileOutputFormat.setOutputPath(fileToHabseJob, new Path(dfsArgs[2]));
        fileToHabseJob.setNumReduceTasks(16);

        //创建HBase的配置对象
        Configuration hbaseConfiguration=HBaseConfiguration.create();

        String zk = "10.99.8.102,10.99.8.103,10.99.8.104";
        String secureid="ulOhQJ2nUfHOGIdHkLAc98mJOGny9cWbkNVM";
        String securekey="swOfKGziXwvfptKVuShn8OPZodEeWE9V";

        hbaseConfiguration.set("hbase.zookeeper.quorum", zk);
        hbaseConfiguration.set("zookeeper.znode.parent", "/hbase-unsecure");

        // 认证时，在创建HBase连接之前，在configuration设置以下两个参数，其他使用方法和HBase开源完全一样,hbase其他接口的使用方法类似，只需要增加以下两个认证参数即可
        hbaseConfiguration.set("hbase.security.authentication.tbds.secureid", secureid);
        hbaseConfiguration.set("hbase.security.authentication.tbds.securekey", securekey);


        //创建目标表对象
        HTable stanTable =new HTable(hbaseConfiguration, dfsArgs[0]);
        HFileOutputFormat.configureIncrementalLoad(fileToHabseJob,stanTable);
        int HdfsToHFileJobResult=fileToHabseJob.waitForCompletion(true)?0:1;

        //当第二个job结束之后，调用BulkLoad方式来将MR结果批量入库
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfiguration);
        //第一个参数为第二个Job的输出目录即保存HFile的目录，第二个参数为目标表
        loader.doBulkLoad(new Path(dfsArgs[2]), stanTable);
        //最后调用System.exit进行退出
        System.exit(HdfsToHFileJobResult);
    }
}
