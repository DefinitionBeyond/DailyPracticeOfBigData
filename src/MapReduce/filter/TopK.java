package MapReduce.filter;

/**
 * @author liutao
 * @date 2018/4/16  22:55
 */

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import MapReduce.Util.MRDPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * 对comment数据集取评论长度为前K的用户ID
 */
public class TopK {
    public final static Integer K = 10;

    public static class SOTopTenMapper extends
            Mapper<Object, Text, NullWritable, Text> {
        // Our output key and value Writables
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Parse the input string into a nice map
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            if (parsed == null) {
                System.out.println(K);
                return;
            }

            String userId = parsed.get("Id");
            Integer reputation = parsed.get("Text").length();
            System.out.println(parsed.get("Text"));
            // Get will return null if the key is not there
            if (userId == null || reputation == null) {
                // skip this record
                return;
            }

            repToRecordMap.put(reputation, new Text(value));

            if (repToRecordMap.size() > K) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class SOTopTenReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

                repToRecordMap.put(parsed.get("Text").length(),
                        new Text(value));

                System.out.println(repToRecordMap.size());
                if (repToRecordMap.size() > K) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            for (Text t : repToRecordMap.descendingMap().values()) {
                System.out.println(t);
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Logger log = Logger.getLogger(TopK.class);
        log.debug("err");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopTenDriver <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Top Ten Users by ID");
        job.setJarByClass(TopK.class);
        job.setMapperClass(SOTopTenMapper.class);
        job.setReducerClass(SOTopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
