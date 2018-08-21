package work5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import work5.hdfs.HdfsDAO;

public class Step5 {

    private static class Step5_Mapper extends Mapper<LongWritable, Text, IntPair, Text> {
        private boolean isStep4 = false;
        private static Map<String, List<String>> result = new HashMap<String, List<String>>();
        private static IntPair k = new IntPair();
        private static Text v = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("|"))
                isStep4 = true;
            String[] lineSplit = Recommend.DELIMITER.split(line);
            List<String> resultList = null;
            // 只输出ID为6的用户的推荐结果
            if (lineSplit[0].equals("6")) {
                if (isStep4) {
                    if (!result.containsKey(lineSplit[0])) {
                        resultList = new ArrayList<String>();
                    } else {
                        resultList = result.get(lineSplit[0]);
                    }
                    String m2 = lineSplit[1];
                    // java中split以"." 、"\"、“|”分隔
                    // 1	101|44.0    -> 1    44.0,101
                    String[] mp2 = m2.split("\\|");
                    resultList.add(mp2[1] + "," + mp2[0]);
                    result.put(lineSplit[0], resultList);
                } else {
                    List<String> tempList = result.get(lineSplit[0]);
                    StringBuilder valString = new StringBuilder();
                    for (int k = 0; k < lineSplit.length; k++) {
                        valString.append(lineSplit[k].split(":")[0]);
                    }
                    for (int j = 0; j < tempList.size(); j++) {
                        String str = tempList.get(j).split(",")[1];
                        if (valString.toString().contains(str)) {//过滤掉已经评过分的数据
                            continue;
                        } else {
                            k.set(Integer.valueOf(lineSplit[0]), Double.valueOf(tempList.get(j).split(",")[0]));//将IntPair作为键进行排序
                            v.set(tempList.get(j).split(",")[1] + "," + tempList.get(j).split(",")[0]);
                            System.out.println(k.getFirst() + "************");
                            context.write(k, v);
                        }
                    }
                }
            }
        }
    }

    private static class Step5_Reducer extends Reducer<IntPair, Text, IntWritable, Text> {

        public void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int k = key.getFirst();
            for (Text val : values) {
                context.write(new IntWritable(k), val);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        String input1 = path.get("Step5Input1");
        String input2 = path.get("Step5Input2");
        String output = path.get("Step5Output");
        Configuration conf = Recommend.getConf();

        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step5.class);

        job.setMapperClass(Step5_Mapper.class);
        job.setReducerClass(Step5_Reducer.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(GroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input1));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//6	101,31.0
//6	104,25.0
//6	106,11.5

