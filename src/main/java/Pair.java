import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Writable, WritableComparable<Pair> {
    private int sum;
    private double avg;

    public Pair(int sum, double avg) {
        this.sum = sum;
        this.avg = avg;
    }

    public Pair() {
    }

    public int compareTo(Pair pair) {
        if (pair.sum == sum && pair.avg == avg) {
            return 0;
        }
        return pair.sum - sum;
    }

    @Override
    public boolean equals(Object p) {
        Pair pair = (Pair)p;
        return pair.sum == sum && pair.avg == avg;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeDouble(avg);
    }

    public void readFields(DataInput dataInput) throws IOException {
        sum = dataInput.readInt();
        avg = dataInput.readDouble();
    }

    public void set(int sum, double avg) {
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public int hashCode() {
        return (int) (sum + Math.round(avg * 1000000));
    }

    @Override
    public String toString() {
        return String.valueOf(sum) + ", " + String.valueOf(avg);
    }}
