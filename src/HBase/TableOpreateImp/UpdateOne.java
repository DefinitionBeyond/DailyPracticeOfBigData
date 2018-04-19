package HBase.TableOpreateImp;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;


import java.io.IOException;

/**
 * @author liutao
 * @date 2018/4/18  19:23
 */
public class UpdateOne {
    private static Connection connection;

    public static void modify(String tableName, String row, String columFamily, String colum, String data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put p = new Put(row.getBytes());
        p.addColumn(columFamily.getBytes(), null, data.getBytes());
        table.put(p);
        table.close();
    }
}
