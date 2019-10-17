package hadoop.relative.frequencies.hybrid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairWritable implements WritableComparable<PairWritable> {

    private Text key;
    private Text value;

    public PairWritable() {
        this.key = new Text();
        this.value = new Text();
    }

    PairWritable(Text key, Text value) {
        this.key = key;
        this.value = value;
    }

    Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    Text getValue() {
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
        if (k != 0) {
            return k;
        }
        return this.value.compareTo(o.value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        PairWritable pair = (PairWritable) object;

        if (!Objects.equals(key, pair.key)) return false;
        return Objects.equals(value, pair.value);
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
