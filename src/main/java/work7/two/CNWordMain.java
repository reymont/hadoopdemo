package work7.two;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CNWordMain {

    public static final String HDFS = "hdfs://172.20.62.34:9000";

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
////        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//
////        if(otherArgs.length != 2){
////            System.err.println("Usage: wordcount <in> <out>");
////            System.exit(2);
////        }
//
//        String dic = HDFS + "/user/hdfs/";
//
//        Job job = Job.getInstance(conf, "CN Word Count");
//        job.setJarByClass(CNWordMain.class);
//        job.setMapperClass(CNWordMapper.class);
//        job.setCombinerClass(CNWordReducer.class); //添加中文功能
//        job.setReducerClass(CNWordReducer.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}