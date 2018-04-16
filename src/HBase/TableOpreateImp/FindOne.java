package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author liutao
 */
public class FindOne {
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();

    /**
     * 查找指定行的数据
     * @param tableName 表名
     * @param row 指定行
     * @throws Exception
     */
    public static void finadOne(String tableName, String row) throws Exception {
        HTable table = new HTable(cfg, tableName);
        // 实例化一个获取单个行相关信息的对象
        Get g = new Get(Bytes.toBytes(row));
        // 查询结果放入结果集
        Result rs = table.get(g);
        System.out.println("Get:" + rs);
    }
}
