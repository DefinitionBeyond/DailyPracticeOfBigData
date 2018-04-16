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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 删除文件或者目录
 */
public class DeleteFile {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/test"; //路径
        Configuration conf = new Configuration(); //配置
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path delef = new Path("hdfs://master:9000/test");
            boolean isDeleted = fs.delete(delef, true);// if delete folder and
            // file cascade
            // boolean isDeleted = fs.delete(delef,false);
            System.out.println(isDeleted);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

