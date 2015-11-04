import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class LineNumberingMapper extends MapReduceBase implements
        org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {
    Text file = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
        String url = reporter.getInputSplit().toString();
        int i = url.lastIndexOf(":");
        String filename = url.substring(0, i);
        System.out.println(filename);
        file.set(filename);
        out.collect(file, value);
    }
}