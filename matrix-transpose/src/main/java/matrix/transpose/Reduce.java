package matrix.transpose;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Reduce extends Reducer<IntWritable, CustomMapWritable, IntWritable, Text> {

    private Logger logger = Logger.getLogger(Reduce.class);

    @Override
    protected void reduce(IntWritable key, Iterable<CustomMapWritable> values, Context context)
            throws IOException, InterruptedException {
        SortedMap<IntWritable, IntWritable> rows = new TreeMap<>();
        for (CustomMapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                rows.put((IntWritable) entry.getKey(), (IntWritable) entry.getValue());
            }
        }
        StringBuilder buffer = new StringBuilder();
        for (IntWritable val : rows.values()) {
            buffer.append(val).append(" ");
        }
        Text result = new Text(buffer.toString().strip());
        logger.info("(" + key + ", " + result + ")");
        context.write(key, result);
    }
}
