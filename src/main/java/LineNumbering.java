import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LineNumbering {
    public static final String AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER = "amountOfLinesInInputFiles";
    public static final String FILES_LIST_PATH = "/home/asd/files.csv";
    public static final String HDFS_URL = "hdfs://asd-H67M-D2:9000";

    public static void main(String[] args) throws IOException, URISyntaxException {

        if(args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }

        //Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(LineNumbering.class);
        conf.setJobName("IPCount");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        conf.setMapperClass(LineNumberingMapper.class);
        conf.setReducerClass(LineNumberingReducer.class);

        //Setting format of input and output
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The hdfs input and output directory to be fetched from the command line
        for (String path: args) {
            FileInputFormat.addInputPaths(conf, path);
        }
        FileOutputFormat.setOutputPath(conf, new Path("out"));

        addFilesMapToCache(conf, args);

        //Running the job
        RunningJob j = JobClient.runJob(conf);


    }

    private static void addFilesMapToCache(JobConf conf, String[] paths) throws URISyntaxException, IOException {
        conf.set("fs.default.name", HDFS_URL);
        FileSystem fileSystem = FileSystem.get(conf);
        // results output
        FSDataOutputStream out = fileSystem.create(new Path(FILES_LIST_PATH));
        for(String path: paths) {
            out.writeBytes(path + "\n");
        }

        DistributedCache.addCacheFile(new URI(FILES_LIST_PATH), conf);
    }
}
