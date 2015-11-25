import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text ip = new Text();
    IntWritable count = new IntWritable();

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), value);
    }

}