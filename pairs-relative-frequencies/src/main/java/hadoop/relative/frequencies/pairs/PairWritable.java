package hadoop.relative.frequencies.pairs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {

    private Text key;
    private Text value;

    public PairWritable() {
        this.key = new Text();
        this.value = new Text();
    }

    public PairWritable(Text key, Text value) {
        this.key = key;
        this.value = value;
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public int compareTo(PairWritable o) {
        int k = this.key.compareTo(o.key);
        if(k != 0) {
            return k;
        }
        return this.value.compareTo(o.value);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + key + ", " + value + ")";
    }
}
