import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class IPCounter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }

        //Creating a JobConf object and assigning a job name for identification purposes
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IPCounter");

        job.setJarByClass(IPCounter.class);
        job.setMapperClass(IPMapper.class);
        //job.setCombinerClass(IPCombiner.class);
        job.setReducerClass(IPReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, LongWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "seq", TextOutputFormat.class, LongWritable.class, Text.class);

        //Running the job
        job.waitForCompletion(true);

    }
}
