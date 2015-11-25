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

    @Override
    protected void reduce(Text ip, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> i = values.iterator();
        while (i.hasNext()) {
            context.write(ip, new IntWritable(i.next().get()));
        }

        mos.write("text", 1, new Text("Hello"));
        mos.write("seq", 2, new Text("Hello2"));
    }
}
