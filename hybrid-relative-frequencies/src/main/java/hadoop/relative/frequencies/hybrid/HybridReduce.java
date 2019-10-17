package hadoop.relative.frequencies.hybrid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HybridReduce extends Reducer<PairWritable, IntWritable, Text, CustomMapWritable> {

    private Logger logger = Logger.getLogger(HybridReduce.class);
    private Map<String, Integer> fStripe;
    private String prev;

    @Override
    protected void setup(Context context) {
        this.fStripe = new HashMap<>();
        this.prev = null;
    }

    @Override
    protected void reduce(PairWritable pair, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String u = pair.getKey().toString();
        String v = pair.getValue().toString();
        if (prev != null && !u.equals(prev)) {
            emit(context);
            fStripe.clear();
        }
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        prev = u;
        fStripe.put(v, sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        emit(context);
    }

    private int total() {
        int sum = 0;
        for (Map.Entry entry : fStripe.entrySet()) {
            sum += (Integer) entry.getValue();
        }
        return sum;
    }

    private void emit(Context context) throws IOException, InterruptedException {
        int total = total();
        CustomMapWritable result = new CustomMapWritable();
        Text u = new Text(prev);
        for (Map.Entry entry : fStripe.entrySet()) {
            Text v = new Text((String) entry.getKey());
            result.put(v, new Text(entry.getValue() + "/" + total));
        }
        logger.info("(" + u + ", " + result + ")");
        context.write(u, result);
    }
}
