package hadoop.relative.frequencies.pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Reduce extends Reducer<StringPairWritable, DoubleWritable, StringPairWritable, DoubleWritable> {

    private Logger logger = Logger.getLogger(Reduce.class);
    private Double sum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.sum = 0D;
    }

    public void reduce(StringPairWritable pair, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double s = 0;
        for (DoubleWritable val : values) {
            s += val.get();
            if (val.equals("*")) sum = s;
            else {
                Double frequency = s / sum;
                logger.info("(" + pair + ", " + frequency + ")");
                context.write(pair, new DoubleWritable(frequency));
            }
        }
    }
}
