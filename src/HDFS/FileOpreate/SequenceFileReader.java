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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Writable;

/**
 * 读取 sequence file
 */
public class SequenceFileReader {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/testsequence";
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            Path path = new Path(uri);
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long positon = reader.getPosition();
            while (reader.next(key, value)) {
                System.out.printf("[%s]\t%s\n", key, value);
                positon = reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
