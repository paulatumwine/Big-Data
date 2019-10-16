package hadoop.relative.frequencies.stripes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reduce extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {

    private Logger logger = Logger.getLogger(Reduce.class);
    private Double sum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.sum = 0D;
    }

    public void reduce(Text key, Iterable<CustomMapWritable> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, Double> common = new HashMap<String, Double>();
        for (CustomMapWritable val : values) {
            for (Map.Entry e: val.entrySet()) {
                String v = e.getKey().toString();
                Double vVal = Double.parseDouble(e.getValue().toString());
                if (common.containsKey(v)) common.put(v, common.get(v) + vVal);
                else common.put(v, vVal);
            }
        }
        Double sum = 0D;
        for (Map.Entry e: common.entrySet()) {
            sum += Double.parseDouble(e.getValue().toString());
        }
        CustomMapWritable fStripe = new CustomMapWritable();
        for (Map.Entry e: common.entrySet()) {
            String v = (String) e.getKey();
            Double vVal = (Double) e.getValue();
            fStripe.put(new Text(v), new DoubleWritable(vVal / sum));
        }
        logger.info("(" + key + ", " + fStripe + ")");
        context.write(key, fStripe);
    }
}
