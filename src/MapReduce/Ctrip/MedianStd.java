package MapReduce.Ctrip;

/**
 * @author liutao
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 对携程数据集按月份进行分组求价格的中位数
 */
public class MedianStd {

    public static class SOMedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable Price = new IntWritable(); //存价格
        private IntWritable Month = new IntWritable(); //存月份

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] itr = value.toString().split(",");
            itr[1] = numDeal(itr[1]); //提取月份

            //数据不完整，数据格式不正确，就退出当前这次数据行操作
            if (!isUpNum(itr[8]) || itr[1] == null || itr[8] == null || itr[8].equals("")) {
                return;
            }
            try {
//                System.out.println(itr[1] + " : " + itr[8]);
                Price.set(Integer.parseInt(itr[8]));//数据集的价格恰好全是int类型的，就用了这个
                Month.set(Integer.parseInt(itr[1]));//月份
                context.write(Month, Price);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return;
            }
        }

        //价格为负的数据是不合理的数据，进行过滤
        public boolean isUpNum(String s) {
            if (s.charAt(0) == '-') {
                return false;
            }
            return true;
        }

        //月份进行处理
        public String numDeal(String s) {
            if (s.charAt(5) >= '0' || s.charAt(5) <= '9') {
                if (s.charAt(5) == '0') { //少于10月的
                    return String.valueOf(s.charAt(6));
                } else {//10 ，11，12月
                    return String.valueOf(s.charAt(5)) + String.valueOf(s.charAt(6));
                }
            }
            return null;
        }
    }

    public static class SOMedianStdDevReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
        private MedianStdDevTuple result = new MedianStdDevTuple();
        private ArrayList<Float> commentLengths = new ArrayList<Float>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            float count = 0;
            commentLengths.clear();
            result.setStdDev(0);

            // Iterate through all input values for this key
            for (IntWritable val : values) {
                commentLengths.add((float) val.get());
                sum += val.get();
                ++count;
            }

            // sort commentLengths to calculate median
            Collections.sort(commentLengths);

            // if commentLengths is an even value, average middle two elements
            if (count % 2 == 0) {
                result.setMedian(
                        (commentLengths.get((int) count / 2 - 1) + commentLengths.get((int) count / 2)) / 2.0f);
            } else {
                // else, set median to middle value
                result.setMedian(commentLengths.get((int) count / 2));
            }

            // calculate standard deviation
            float mean = sum / count;

            float sumOfSquares = 0.0f;
            for (Float f : commentLengths) {
                sumOfSquares += (f - mean) * (f - mean);
            }

            result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MedianStdDevDriver <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "StackOverflow Comment Length Median StdDev By Hour");
        job.setJarByClass(MedianStd.class);
        job.setMapperClass(SOMedianStdDevMapper.class);
        job.setReducerClass(SOMedianStdDevReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStdDevTuple.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MedianStdDevTuple implements Writable {
        private float median = 0;
        private float stddev = 0f;

        public float getMedian() {
            return median;
        }

        public void setMedian(float median) {
            this.median = median;
        }

        public float getStdDev() {
            return stddev;
        }

        public void setStdDev(float stddev) {
            this.stddev = stddev;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            median = in.readFloat();
            stddev = in.readFloat();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeFloat(median);
            out.writeFloat(stddev);
        }

        @Override
        public String toString() {
            return median + "\t" + stddev;
        }
    }
}
