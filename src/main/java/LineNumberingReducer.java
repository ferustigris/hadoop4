import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class LineNumberingReducer extends MapReduceBase implements
        org.apache.hadoop.mapred.Reducer<Text, Text, LongWritable, Text> {
    ArrayList<String> paths = new ArrayList<String>();
    LongWritable ln = new LongWritable();

    public void configure(JobConf job) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(job);
            // results output
            FSDataInputStream in = fileSystem.open(new Path(LineNumbering.FILES_LIST_PATH));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while (line != null) {
                paths.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reduce(Text filename, Iterator<Text> values, OutputCollector<LongWritable, Text> out, Reporter reporter) throws IOException {
        long lineNumber = 0;
        for(String path: paths) {
            if (filename.toString().contains(path)) {
                break;
            }
            lineNumber += reporter.getCounter(LineNumbering.AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER, path).getValue();

        }

        while (values.hasNext()) {
            //Dumping the output
            ln.set(lineNumber);
            out.collect(ln, values.next());
            lineNumber++;
        }

    }
}
