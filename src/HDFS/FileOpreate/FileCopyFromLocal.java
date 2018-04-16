package HDFS.FileOpreate;

/**
 * @author liutao
 */

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URI;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 把本地文件上传到hdfs
 */
public class FileCopyFromLocal {
    public static void main(String[] args) throws IOException {
        String source = "/home/liutao/text123"; // 本地文件路径
        String destination = "hdfs://master:9000/input/text456"; //hdfs路径
        InputStream in = new BufferedInputStream(new FileInputStream(source)); //将本地文件读入缓冲区
        Configuration conf = new Configuration();//得到配置
        FileSystem fs = FileSystem.get(URI.create(destination), conf);
        OutputStream out = fs.create(new Path(destination)); //创建hdfs文件
        IOUtils.copyBytes(in, out, 4096, true); //将缓冲区的内容写入hdfs文件
    }
}
