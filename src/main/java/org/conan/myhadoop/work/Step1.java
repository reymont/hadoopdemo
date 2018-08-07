package org.conan.myhadoop.work;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.conan.myhadoop.work.hdfs.HdfsDAO;

public class Step1 {
	
	public static class Step1_ToItemPrefMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		private static IntWritable k = new IntWritable();
		private static Text out = new Text();
		
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String line = value.toString();
			String[] lineSplit = Recommend.DELIMITER.split(line);
			// 将第一列作为键
			k.set(Integer.parseInt(lineSplit[0]));
			// 将第2列和第3列用:合并，一起作为值
            // 1,101,5.0        1   101:5.0
            // 1,102,3.0    ->  1   102,3.0
            // 1,103,2.5        1   103,2.5
			out.set(lineSplit[1]+":"+lineSplit[2]);
			context.write(k, out);
		}
	}
	
	public static class Step1_ToUserVectorReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		private static Text out = new Text();
		public void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			StringBuilder output =new StringBuilder();
			for(Text val:values){
				output.append(","+val);
			}
			// 将同一个用户的评分结果合并
			// 1	103:2.5,101:5.0,102:3.0
			out.set(output.toString().replaceFirst(",", ""));
			context.write(key, out);
		}
	}
	
	public static void run(Map<String,String> path)throws IOException, InterruptedException, ClassNotFoundException{
		
		Configuration conf = Recommend.getConf();
		String input = path.get("Step1Input");
		String output = path.get("Step1Output");
		
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(path.get("data"),input);
		
		Job job = new Job(conf);
        //Job job = Job.getInstance(conf, "Step1");
		job.setJarByClass(Step1.class);
		
		job.setMapperClass(Step1_ToItemPrefMapper.class);
		job.setReducerClass(Step1_ToUserVectorReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}

// 用户评分结果矩阵
// 1	103:2.5,101:5.0,102:3.0
// 2	101:2.0,102:2.5,103:5.0,104:2.0
// 3	107:5.0,101:2.0,104:4.0,105:4.5
// 4	103:3.0,106:4.0,104:4.5,101:5.0
// 5	101:4.0,102:3.0,103:2.0,104:4.0,105:3.5,106:4.0
// 6	102:4.0,103:2.0,105:3.5,107:4.0