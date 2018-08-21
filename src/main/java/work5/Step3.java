package work5;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import work5.hdfs.HdfsDAO;

public class Step3 {
	
	public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		private static IntWritable k = new IntWritable();
		private static Text out = new Text();
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String line = value.toString();
			String[] lineSplit = Recommend.DELIMITER.split(line);
            // 1	103:2.5,101:5.0,102:3.0
            // 按制表符\t或逗号,切分
            // 获取用户ID
			String userId = lineSplit[0];
            //从1开始取值，去掉用户ID
            //103:2.5,101:5.0,102:3.0
            //输出
            //103   1:2.5
            //101   1:5.0
            //102   1:3.0
			for(int i=1;i<lineSplit.length;i++){
				String itemId = lineSplit[i].toString().split(":")[0];
				String score = lineSplit[i].toString().split(":")[1];
				k.set(Integer.valueOf(itemId));
				out.set(userId+':'+score);
				context.write(k, out);
			}
		}
	}

	public static void run1(Map<String,String> path) throws IOException, InterruptedException, ClassNotFoundException{
		String input = path.get("Step3Input1");
		String output = path.get("Step3Output1");
		
		Configuration conf = Recommend.getConf();
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.rmr(output);
		
		Job job = new Job(conf);
		job.setJarByClass(Step3.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}

//	物品评分矩阵数据：
//			101	4:5.0
//			101	3:2.0
//			101	5:4.0
//			101	2:2.0
//			101	1:5.0
//			102	1:3.0
//			102	2:2.5
//			102	5:3.0
//			102	6:4.0
//			103	5:2.0
//			103	1:2.5
//			103	4:3.0
//			103	2:5.0
//			103	6:2.0
//			104	3:4.0
//			104	4:4.5
//			104	5:4.0
//			104	2:2.0
//			105	3:4.5
//			105	5:3.5
//			105	6:3.5
//			106	5:4.0
//			106	4:4.0
//			107	6:4.0
//			107	3:5.0


	public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String line = value.toString();
			String[] lineSplit = Recommend.DELIMITER.split(line);
			//与Step2Output输出的值一致
			context.write(new Text(lineSplit[0]), new IntWritable(Integer.valueOf(lineSplit[1])));
		}
	}
	
	public static void run2(Map<String,String> path) throws IOException, InterruptedException, ClassNotFoundException{
		String input = path.get("Step3Input2");
		String output = path.get("Step3Output2");
		
		Configuration conf = new Configuration();
		
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.rmr(output);
		
		Job job = new Job(conf);
		job.setJarByClass(Step3.class);
		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}
