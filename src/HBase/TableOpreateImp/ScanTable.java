package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @date 2018/4/16  20:38
 */
public class ScanTable {
    //得到Hbase连接对象
    private static Connection connection;
    // 使用默认的HBase配件文件创建配置
    static Configuration cfg = HBaseConfiguration.create();

    public static void scan(String tableName) throws IOException {
        HTable table = new HTable(cfg, tableName);
        // 创建全表扫描对象
        Scan s = new Scan();
        // 得到结果集
        ResultScanner rs = table.getScanner(s);
        // 对结果集的数据进行遍历
        rs.forEach((r) -> {
//            System.out.println("scan:" + r);
            showCell(r);
        });
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        //对指定表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建全表扫描对象
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            showCell(result);
        }
        table.close();
    }

    private static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " "); //输出Row
            System.out.println("Timetamp:" + cell.getTimestamp() + " "); // 输出时间戳
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " "); //对应列族
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " "); //对应列
            System.out.println("data:" + new String(CellUtil.cloneValue(cell)) + " "); //对应的数据
        }
    }
}
