package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @date 2018/4/16  20:38
 */
public class ScanTable {
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();
    public static void scan(String tableName) throws Exception {
        HTable table = new HTable(cfg, tableName);
        // 创建全表扫描对象
        Scan s = new Scan();
        // 得到结果集
        ResultScanner rs = table.getScanner(s);
        // 对结果集的数据进行遍历
        rs.forEach((r) -> {
            System.out.println("scan:" + r);
        });

    }
}
