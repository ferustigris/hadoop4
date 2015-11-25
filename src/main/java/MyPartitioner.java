import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner implements Partitioner {
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }

    public void configure(JobConf jobConf) {

    }
}
