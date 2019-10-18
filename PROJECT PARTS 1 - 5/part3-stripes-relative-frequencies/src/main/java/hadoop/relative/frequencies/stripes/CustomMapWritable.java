package hadoop.relative.frequencies.stripes;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Set;

public class CustomMapWritable extends MapWritable {

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        result.append("["); int cnt = 1;
        for (Object key : keySet) {
            result.append(key.toString() + ": " + this.get(key));
            if (cnt < keySet.size()) result.append(", ");
            cnt++;
        }
        result.append("]");
        return result.toString();
    }
}
