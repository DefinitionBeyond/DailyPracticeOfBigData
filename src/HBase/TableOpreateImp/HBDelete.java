package HBase.TableOpreateImp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author liutao
 */
public class HBDelete {
    // 使用默认的HBase配件文件创建配置


    static Configuration cfg = HBaseConfiguration.create();

    /**
     * 根据表名删除表
     *
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

    //连接对象
    private static Connection connection;

    /**
     * 删除指定行
     *
     * @param tableName 表名
     * @param row       指定行
     * @return
     * @throws IOException
     */
    public static boolean deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = null; //删除对象
        try {
            delete = new Delete(row.getBytes());//添加删除的行
            table.delete(delete);
        } catch (IOException e) {
            return false;
        } finally {
            table.close();
        }
        return true;
    }

    /**
     * 删除指定列族
     *
     * @param tableName   表名
     * @param row         行
     * @param columFamily 列族
     * @return
     * @throws IOException
     */
    public static boolean deleteColumFamily(String tableName, String row, String columFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = null;
        try {
            delete = new Delete(row.getBytes());
            delete.addFamily(columFamily.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            return false;
        } finally {
            table.close();
        }
        return true;
    }

    /**
     * 删除指定列的元素
     *
     * @param tableName   表名
     * @param row         行
     * @param columFamily 列族
     * @param colum       列
     * @return
     * @throws IOException
     */
    public static boolean deleteColum(String tableName, String row, String columFamily, String colum) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = null;
        try {
            delete = new Delete(row.getBytes());
            delete.addFamily(columFamily.getBytes());
            delete.addColumn(columFamily.getBytes(), colum.getBytes());
            table.delete(delete);
        } catch (IOException e) {
            return false;
        } finally {
            table.close();
        }
        return true;
    }
}
