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
        System.out.println(reporter.getInputSplit());
        reporter.getCounter(LineNumbering.AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER, reporter.getInputSplit().toString()).increment(1);
        file.set(reporter.getInputSplit().toString());
        out.collect(file, value);
    }
}