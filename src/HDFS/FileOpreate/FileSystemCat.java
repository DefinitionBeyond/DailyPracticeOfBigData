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
 * 打开指定文件的内容
 *
 * 执行指令格式：hadoop jar [jar名] [public类的相对项目的路径]
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9000/input/wordtest"; //指定路径
        Configuration conf = new Configuration(); //得到hdfs配置
        FileSystem fs = FileSystem.get(URI.create(uri), conf); //根据配置的到hdfs文件
        InputStream in = null;
        try {
            in = fs.open(new Path(uri)); //打开文件内容
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
