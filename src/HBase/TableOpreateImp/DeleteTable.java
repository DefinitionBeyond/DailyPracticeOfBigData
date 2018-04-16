package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @author liutao
 */
public class DeleteTable {
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();

    /**
     * 根据表名删除表
     * @param tableName 表名
     * @throws Exception
     */
    public static void deleteTable(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            // 将表设为无效
            admin.disableTable(tableName);
            // 删除表
            admin.deleteTable(tableName);
            System.out.println("drop " + tableName + " success!");
        } else {
            System.out.println(tableName + " table is not exists!");
        }
    }
}
