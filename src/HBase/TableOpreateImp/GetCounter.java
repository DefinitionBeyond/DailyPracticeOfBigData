package HBase.TableOpreateImp;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author liutao
 * @date 2018/4/18  20:41
 */
public class GetCounter {
    private Connection connection;
    private static int num = 0;

    /**
     * 统计表中数据的条数
     *
     * @param tableName
     * @throws IOException
     */
    public void getWC(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        synchronized (this) { //计数
            rs.forEach((r) -> {
                num++;
            });
        }
    }

    public static int getNum() {
        return num;
    }
}
