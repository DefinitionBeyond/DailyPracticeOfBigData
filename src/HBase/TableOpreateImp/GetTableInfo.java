package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @author liutao
 * @date 2018/4/18  20:22
 */
public class GetTableInfo {
    private static Connection connection;
    private static Configuration configuration = HBaseConfiguration.create();

    /**
     * 得到Hbase数据库所有表的信息
     *
     * @throws IOException
     */
    public static void getHbaseInfo() throws IOException {
        Admin admin = new HBaseAdmin(configuration);
        HTableDescriptor[] hTableDescriptor = admin.listTables();
        for (HTableDescriptor tableDescriptor : hTableDescriptor) {
            System.out.println("tableName:" + tableDescriptor.getNameAsString());
        }
    }

    public static void getTableInfo(String tableName) throws IOException {
        Admin admin = new HBaseAdmin(configuration);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        System.out.println("tableName:" + hTableDescriptor.getNameAsString());
    }
}
