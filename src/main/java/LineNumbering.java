import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Counter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LineNumbering {
    public static final String AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER = "amountOfLinesInInputFiles";
    public static final String LINES_AMOUNTS_MAP = "./lines_amount_map.csv";

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if(args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }

        //Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(LineNumbering.class);
        conf.setJobName("LinesAmount");

        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        conf.setMapperClass(LinesAmountMapper.class);

        //Setting format of input and output
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The hdfs input and output directory to be fetched from the command line
        for (String path: args) {
            FileInputFormat.addInputPaths(conf, path);
        }
        FileOutputFormat.setOutputPath(conf, new Path("out"));

        //Running the job
        RunningJob j = JobClient.runJob(conf);

        File f = new File(LINES_AMOUNTS_MAP);

        BufferedWriter out = new BufferedWriter(new FileWriter(f));
        for (Counter c: j.getCounters().getGroup(AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER)) {
            out.write(c.getDisplayName() + " " + c.getValue() + "\n");
        }
        out.close();

        DistributedCache.addCacheFile(new URI(LINES_AMOUNTS_MAP), conf);

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(LineNumberingMapper.class);
        conf.setReducerClass(LineNumberingReducer.class);
        FileOutputFormat.setOutputPath(conf, new Path("out2"));

        JobClient.runJob(conf);
    }
}
