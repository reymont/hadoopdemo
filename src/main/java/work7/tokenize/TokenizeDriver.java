package work7.tokenize;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.conan.myhadoop.hdfs.HdfsDAO;
import work7.tokenize.inputformat.MyInputFormat;

import java.io.IOException;


public class TokenizeDriver {
	public static final String HDFS = "hdfs://172.20.62.34:9000";

	public static void main(String[] args) throws Exception {
		
		// set configuration
		JobConf conf = new JobConf(TokenizeDriver.class);
		conf.setJobName("Tokenizde");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		//conf.addResource("classpath:/hadoop/mapred-site.xml");

		Job job = new Job(conf);
		job.setJarByClass(TokenizeDriver.class);

	    // specify input format
		job.setInputFormatClass(MyInputFormat.class);
		
        //  specify mapper
		job.setMapperClass(TokenizeMapper.class);
		
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String dic = HDFS + "/user/hdfs/";
		String digital = HDFS + "/user/hdfs/digital";

		HdfsDAO hdfsDao = new HdfsDAO(TokenizeDriver.HDFS, conf);
		hdfsDao.rmr(digital);
		hdfsDao.mkdirs(digital);
		hdfsDao.copyFile("digital/camera/camera3", digital);

		// specify input and output DIRECTORIES 
		Path inPath = new Path(digital);
		Path outPath = new Path(HDFS + "/user/hdfs/output");
		try {                                            //  input path
			FileSystem fs = inPath.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(inPath);
			for(int i=0; i<stats.length; i++)
				FileInputFormat.addInputPath(job, stats[i].getPath());
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}			
        FileOutputFormat.setOutputPath(job,outPath);     //  output path

		// delete output directory
		try{
			FileSystem hdfs = outPath.getFileSystem(conf);
			if(hdfs.exists(outPath))
				hdfs.delete(outPath);
			hdfs.close();
		} catch (Exception e){
			e.printStackTrace();
			return ;
		}
		
		//  run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
