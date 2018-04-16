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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * 写入 sequence file
 *
 *  Sequence 文件是一种特殊类型的二进制文件，将<key, value>序列化到文件中
 *
 */
public class SequenceFileWriter {
    private static final String[] text = { "hello hadoop", "bye hadoop", };

    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/testsequence";//写入的路径
        Configuration conf = new Configuration(); //得到配置
        SequenceFile.Writer writer = null; //写
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path path = new Path(uri);
            IntWritable key = new IntWritable();
            Text value = new Text();
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass()); //根据配置，路径，key-value的格式得到写对象
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);//写key
                value.set(text[i % text.length]);//写value
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}
