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

public class CreateDir {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/test"; //创建hdfs目录的路径
        Configuration conf = new Configuration(); //得到配置
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path dfs = new Path("hdfs://master:9000/test");
            fs.mkdirs(dfs); //创建目录
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
