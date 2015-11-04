import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class LineNumberingReducer extends MapReduceBase implements
        org.apache.hadoop.mapred.Reducer<Text, Text, LongWritable, Text> {
    Map<String, Integer> paths = new TreeMap<String, Integer>();
    LongWritable ln = new LongWritable();

    public void configure(JobConf job) {
        System.out.println("conf");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(LineNumbering.LINES_AMOUNTS_MAP))));
            String line = reader.readLine();
            while (line != null) {
                System.out.println("line: " + line);
                String rec[] = line.split("\\s");
                String path = rec[0];
                Integer count = Integer.valueOf(rec[1]);
                paths.put(path, count);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reduce(Text filename, Iterator<Text> values, OutputCollector<LongWritable, Text> out, Reporter reporter) throws IOException {
        long lineNumber = 0;
        for(String path: paths.keySet()) {
            System.out.println("path    : " + path);
            System.out.println("filename: " + filename);
            if (filename.toString().equals(path)) {
                System.out.println("break!");
                break;
            }
            lineNumber += paths.get(path);
            System.out.println("lineNumber: " + lineNumber);

        }

        while (values.hasNext()) {
            //Dumping the output
            ln.set(lineNumber);
            out.collect(ln, values.next());
            lineNumber++;
        }

    }
}
