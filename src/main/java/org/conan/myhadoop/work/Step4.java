package org.conan.myhadoop.work;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

public class Step4 {
//	public static final Map<String,List<String>> existsItem = new HashMap<String,List<String>>();

    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static Map<String, List<String[]>> cooccurrenceMatrix = new HashMap<String, List<String[]>>();
        private static IntWritable k = new IntWritable();
        private static Text out = new Text();

        public void map(LongWritable kwy, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 物品同现矩阵数据：
            // 101:101	5
            // lineSplit[0].split(":").length = 2
            // 评分矩阵
            // 101	4:5.0
            // lineSplit[0].split(":").length = 1

            String[] lineSplit = Recommend.DELIMITER.split(line);
            String[] lineSplit1 = lineSplit[0].split(":");
            String[] lineSplit2 = lineSplit[1].split(":");
            List<String[]> cooccurrenceMatrixList = null;
            if (lineSplit1.length > 1) {                      //物品同现矩阵
                // lineSplit    ->  101:101	5
                // lineSplit1   ->  101:101
                // lineSplit2   ->  5

                if (!cooccurrenceMatrix.containsKey(lineSplit1[0])) {
                    cooccurrenceMatrixList = new ArrayList<String[]>();
                } else {
                    cooccurrenceMatrixList = cooccurrenceMatrix.get(lineSplit1[0]);
                }
                // cooccurrence ->  101,5
                // cooccurrenceMatrix   ->  101 101,5
                String[] cooccurrence = {lineSplit1[1], lineSplit[1]};
                cooccurrenceMatrixList.add(cooccurrence);
                cooccurrenceMatrix.put(lineSplit1[0], cooccurrenceMatrixList);
            } else {                                      //物品评分矩阵
                // lineSplit    ->  101	4:5.0
                // lineSplit1   ->  101
                // lineSplit2   ->  4:5.0

                List<String[]> tempList = cooccurrenceMatrix.get(lineSplit[0]);
                k.set(Integer.valueOf(lineSplit2[0]));//用户ID作为key

                for (int i = 0; i < tempList.size(); i++) {
                    String[] cooccurrence = tempList.get(i);
                    //评分与物品同现次数相乘
                    out.set(cooccurrence[0] + "," + Double.valueOf(lineSplit2[1]) * Integer.valueOf(cooccurrence[1]));
                    context.write(k, out);
                }
            }
        }
    }

    public static class Step4_AggregateAndRecommendReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private static Text out = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> result = new HashMap<String, Double>();
            for (Text val : values) {
                String[] valSplit = val.toString().split(",");
                if (result.containsKey(valSplit[0])) {
                    result.put(valSplit[0], result.get(valSplit[0]) + Double.valueOf(valSplit[1]));
                } else {
                    result.put(valSplit[0], Double.valueOf(valSplit[1]));
                }
            }
            Iterator<String> iter = result.keySet().iterator();
            while (iter.hasNext()) {//得到每个用户的每个物品的评分
                String itemId = iter.next();
                out.set(itemId + "|" + result.get(itemId));
                context.write(key, out);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.getConf();
        String input1 = path.get("Step3Output1");
        String input2 = path.get("Step3Output2");
        String output = path.get("Step4Output");

        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step4.class);

        job.setMapperClass(Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_AggregateAndRecommendReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//			101	1:5.0
//			102	1:3.0
//			103	1:2.5

// 101:101	5
// 101:102	3
// 101:103	4

// 5*5+3*3+2.5*4=44 -> 1    101|44.0




//按用户分组的所有物品评分结果数据：
//        1	107|10.5
//        1	106|18.0
//        1	105|21.0
//        1	104|33.5
//        1	103|44.5
//        1	102|37.0
//        1	101|44.0
//        2	107|11.5
//        2	106|20.5
//        2	105|23.0
//        2	104|36.0
//        2	103|49.0
//        2	102|40.0
//        2	101|45.5
//        3	107|25.0
//        3	106|16.5
//        3	105|35.5
//        3	104|38.0
//        3	103|34.0
//        3	102|28.0
//        3	101|40.0
//        4	107|12.5
//        4	106|33.0
//        4	105|29.0
//        4	104|55.0
//        4	103|56.5
//        4	102|40.0
//        4	101|63.0
//        5	107|20.0
//        5	106|34.5
//        5	105|40.5
//        5	104|59.0
//        5	103|65.0
//        5	102|51.0
//        5	101|68.0
//        6	107|21.0
//        6	106|11.5
//        6	105|30.5
//        6	104|25.0
//        6	103|37.0
//        6	102|35.0
//        6	101|31.0
