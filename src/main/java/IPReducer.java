import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

public class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    Pair p = new Pair();
    private MultipleOutputs mos;

    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    public void reduce(Text ip, Iterator<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        while (values.hasNext()) {
            context.write(ip, new IntWritable(values.next().get()));
        }

        mos.write("text", 1, new Text("Hello"));
        mos.write("seq", 2, new Text("Hello2"));
    }
}
