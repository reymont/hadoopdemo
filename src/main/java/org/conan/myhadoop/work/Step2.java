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

public class Step2 {

	public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private static Text k = new Text();
		private static IntWritable out = new IntWritable(1);
		
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String line = value.toString();
			//1	103:2.5,101:5.0,102:3.0
            //按制表符\t或,切分
			String[] lineSplit = Recommend.DELIMITER.split(line);
			//分割后，从1开始取值，去掉用户ID
            //103:2.5,101:5.0,102:3.0
			for(int i=1;i<lineSplit.length;i++){
			    //103:2.5,101:5.0,102:3.0
                //按:切分，去掉评分
                //103,101,102
                //将电影编组，按出现的方式显示
                //101:101   1
                //101:102   1
                //101:103   1
                //102:101   1
                //102:102   1
                //102:103   1
                //103:101   1
                //103:102   1
                //103:103   1
				String itemId = lineSplit[i].split(":")[0];
				for(int j=1;j<lineSplit.length;j++){
					String itemId1 = lineSplit[j].split(":")[0];
					k.set(itemId+":"+itemId1);
					context.write(k, out);
				}
			}
		}
	}
	
	public static class Step2_UserVectorToConoccurrenceReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void run(Map<String,String> path)throws IOException, InterruptedException, ClassNotFoundException{
		String input = path.get("Step2Input");
		String output = path.get("Step2Output");
		
		Configuration conf = Recommend.getConf();
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.rmr(output);
		
		Job job = new Job(conf);
		job.setJarByClass(Step2.class);
		
		job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
		job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
// 物品同现矩阵数据：
// 101:101	5
// 101:102	3
// 101:103	4
// 101:104	4
// 101:105	2
// 101:106	2
// 101:107	1
// 102:101	3
// 102:102	4
// 102:103	4
// 102:104	2
// 102:105	2
// 102:106	1
// 102:107	1
// 103:101	4
// 103:102	4
// 103:103	5
// 103:104	3
// 103:105	2
// 103:106	2
// 103:107	1
// 104:101	4
// 104:102	2
// 104:103	3
// 104:104	4
// 104:105	2
// 104:106	2
// 104:107	1
// 105:101	2
// 105:102	2
// 105:103	2
// 105:104	2
// 105:105	3
// 105:106	1
// 105:107	2
// 106:101	2
// 106:102	1
// 106:103	2
// 106:104	2
// 106:105	1
// 106:106	2
// 107:101	1
// 107:102	1
// 107:103	1
// 107:104	1
// 107:105	2
// 107:107	2