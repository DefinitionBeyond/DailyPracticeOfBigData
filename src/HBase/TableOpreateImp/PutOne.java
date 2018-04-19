package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author liutao
 */

/**
 * 插入一行数据
 */
public class PutOne {
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();

    /**
     *
     * @param tableName 表名
     * @param row 行
     * @param columFamily 列族
     * @param column 列
     * @param data 数据
     * @throws IOException
     */
    public static void add(String tableName, String row, String columFamily, String column, String data)
            throws IOException {

        // 实例一个与Hbase表进行通信的对象
        // 对于多线程的时候，可能会崩溃，建议使用HTablePool
        HTable table = new HTable(cfg, tableName);
        // Put 对单个行进行添加操作
        Put p = new Put(Bytes.toBytes(row));
        p.add(Bytes.toBytes(columFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(p);// 向表中添值
        System.out.println("add " + row + "cloumFamily " + data + " success!");
    }

    /**
     * @param tableName   表名
     * @param row         行
     * @param columFamily 多个列族
     * @param column      多个列
     * @param data        每个列指定的数据
     * @throws IOException
     */
    public static void add(String tableName, String row, String[] columFamily, String[] column, String[] data)
            throws IOException {
        HTable table = new HTable(cfg, tableName);
        Put p = new Put(row.getBytes());
        for (int i = 0; i < columFamily.length; i++) {
            p.add(columFamily[i].getBytes(), column[i].getBytes(), data[i].getBytes());
        }
        table.put(p);
    }

}
