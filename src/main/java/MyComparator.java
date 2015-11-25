import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        System.out.println(tp1.toString());
        return 0;
    }
}
