package hadoop.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(Reduce.class);

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        double cnt = 0;
        for (MapWritable val : values) {
            for (Map.Entry e : val.entrySet()) {
                sum += Double.parseDouble(e.getKey().toString());
                cnt += Double.parseDouble(e.getValue().toString());
            }
        }
        double avg = sum / cnt;
        logger.info("(" + key + ", " + avg + ")");
        context.write(key, new DoubleWritable(avg));
    }
}
