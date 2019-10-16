package hadoop.relative.frequencies.stripes;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringPairWritable implements WritableComparable {

    private String key;
    private String value;

    public StringPairWritable() {
    }

    StringPairWritable(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public void setStringey(String key) {
        this.key = key;
    }

    public void setStringalue(String value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(this.key);
        out.writeChars(this.value);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readLine();
        value = in.readLine();
    }

    @Override
    public String toString() {
        return "< " + key + " , " + value + " >";
    }
}
