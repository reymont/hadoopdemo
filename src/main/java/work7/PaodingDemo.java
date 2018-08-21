package work7;

import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import java.io.*;

public class PaodingDemo {

//    public static void main(String[] args) throws IOException {
//
//        String content = null;
//
//        // 将庖丁封装成符合Lucene要求的Analyzer规范
//        Analyzer analyzer = new PaodingAnalyzer();
//
//        FileInputStream in = null;
//        in = new FileInputStream(new File("digital/camera/camera3"));
//
//        InputStreamReader inReader = new InputStreamReader(in);
//        BufferedReader br = new BufferedReader(inReader);
//        TokenStream ts = analyzer.tokenStream(content, br);
//
//        Token t;
//        t = ts.next();
//        while (t != null) {
//            content += t.termText() + " ";
//            System.out.println(t.termText());
//            t = ts.next();
//        }
//    }
}
