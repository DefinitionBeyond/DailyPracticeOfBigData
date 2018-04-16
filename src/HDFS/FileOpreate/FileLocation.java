package HDFS.FileOpreate;

/**
 * @author liutao
 */
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URI;
import java.net.MalformedURLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 查看hdfs文件的文件位置
 */
public class FileLocation {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/wordtest";
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path fpath = new Path(uri);
            FileStatus filestatus = fs.getFileStatus(fpath); //得到文件状态
            BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());//每个文件块的信息
            int blockLen = blkLocations.length;
            for (int i = 0; i < blockLen; i++) {
                String[] hosts = blkLocations[i].getHosts();//主机名
                System.out.println("block_" + i + "_location:" + hosts[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}