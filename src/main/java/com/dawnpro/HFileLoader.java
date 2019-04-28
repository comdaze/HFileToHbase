package com.dawnpro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by shaobo on 15-6-9.
 */
public class HFileLoader {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: TripTaggerMan <input> <tablename>");
            System.exit(2);
        }
        doBulkLoad(args[0],args[1]);

    }



    public static void doBulkLoad(String pathToHFile, String tableName){
        try {
            Configuration configuration = HBaseConfiguration.create();
            String zk = "10.99.8.102,10.99.8.103,10.99.8.104";
            String secureid="*";
            String securekey="*";

            configuration.set("hbase.zookeeper.quorum", zk);
            configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

            // 认证时，在创建HBase连接之前，在configuration设置以下两个参数，其他使用方法和HBase开源完全一样,hbase其他接口的使用方法类似，只需要增加以下两个认证参数即可
            configuration.set("hbase.security.authentication.tbds.secureid", secureid);
            configuration.set("hbase.security.authentication.tbds.securekey", securekey);

            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
            HTable hTable = new HTable(configuration, tableName);//指定表名
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);//导入数据
            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }

    }

}
