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
 * 检查文件是否存在
 */
public class CheckFileIsExist {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/text123"; //文件路径
        Configuration conf = new Configuration();//得到配置
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path path = new Path(uri); //实例化一个文件对象
            boolean isExists = fs.exists(path); //是否存在实例化对象
            System.out.println(isExists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
