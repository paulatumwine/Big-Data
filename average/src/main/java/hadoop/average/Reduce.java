package hadoop.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(Reduce.class);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        double cnt = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            cnt++;
        }
        double avg = sum / cnt;
        logger.info("(" + key + ", " + avg + ")");
        context.write(key, new DoubleWritable(avg));
    }
}
