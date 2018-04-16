package HBase.TableOpreateImp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @author liutao
 */

/**
 * 创建表
 */
public class CreateTable {
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();

    /**
     * @param tableName  表名
     * @param columFamily 列族
     * @throws IOException
     */
    public static void create(String tableName, String columFamily) throws IOException {
        // 如果不是使用hbase的zookeeper集群，则需要指定zookeeper集群的位置
        // cfg.set("hbase.zookeeper.quorum", "master，slave，slavex");
        // 创建一个接口来管理HBase数据库的表信息
        HBaseAdmin admin = new HBaseAdmin(cfg);
        // 如果表存在
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            System.exit(0);// 退出
        } else {
            // HTableDescriptor包含了表的名字和对应的列族
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            // 通过HColumnDescriptor的一个市里，为HTableDescriptor添加一个列族
            tableDesc.addFamily(new HColumnDescriptor(columFamily));
            // 创建表
            admin.createTable(tableDesc);
            System.out.println("create Table success!");
        }
    }
}
