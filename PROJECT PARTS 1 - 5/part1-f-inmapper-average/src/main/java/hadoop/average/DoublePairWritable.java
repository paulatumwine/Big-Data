package hadoop.average;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoublePairWritable implements WritableComparable {

    private Double key;
    private Double value;

    public DoublePairWritable() {
    }

    DoublePairWritable(Double key, Double value) {
        this.key = key;
        this.value = value;
    }

    Double getKey() {
        return key;
    }

    public void setKey(Double key) {
        this.key = key;
    }

    Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.key);
        out.writeDouble(this.value);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readDouble();
        value = in.readDouble();
    }

    @Override
    public String toString() {
        return "< " + key + " , " + value + " >";
    }
}
