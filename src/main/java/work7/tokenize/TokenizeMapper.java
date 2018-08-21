package work7.tokenize;

import java.io.*;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class TokenizeMapper extends Mapper<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	PaodingAnalyzer analyzer = new PaodingAnalyzer();

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        // set key
        outKey.set(key);

        // set value
        String line = value.toString();
        StringReader sr = new StringReader(line);
        TokenStream ts = analyzer.tokenStream("", sr);
        StringBuilder sb = new StringBuilder();
        try{
            Token t;
            while ((t = ts.next()) != null){
                sb.append(t.termText());
                sb.append(" ");
            }
        }catch(Exception e){
            context.getCounter(Counter.FAILDOCS).increment(1);
        }
        outValue.set(sb.toString());

        //  output keyvalue pair
        context.write(outKey, outValue);
    }
}
