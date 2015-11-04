import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class LineNumberingReducer extends MapReduceBase implements
        org.apache.hadoop.mapred.Reducer<Text, Text, LongWritable, Text> {
    ArrayList<String> paths = new ArrayList<String>();
    LongWritable ln = new LongWritable();

    public void configure(JobConf job) {
        System.out.println("conf");
        try {
            File f = new File("./files");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            String line = reader.readLine();
            while (line != null) {
                System.out.println("line: " + line);
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
            System.out.println("path    : " + path);
            System.out.println("filename: " + filename);
            if (filename.toString().equals(path)) {
                System.out.println("break!");
                break;
            }
            lineNumber += reporter.getCounter(LineNumbering.AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER, path).getValue();
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
