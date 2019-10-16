package hadoop.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

    private Logger logger = Logger.getLogger(Map.class);
    private HashMap<String, Pair<Double, Double>> associativeArray;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        associativeArray = new HashMap<String, Pair<Double, Double>>();
        logger.info("initialised in mapper cache successfully");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        String bts = words[words.length - 1];
        if (!bts.matches("\\d+")) return;
        Double bytes = Double.parseDouble(bts);

        String holder = words[0];
        logger.info("(" + holder + ", " + bytes + ")");

        Pair<Double, Double> pair = new Pair(bytes, 1D);
        if (associativeArray.containsKey(holder)) {
            Double k = associativeArray.get(holder).getKey() + pair.getKey();
            pair = new Pair(k, associativeArray.get(holder).getValue() + 1D);
        }

        associativeArray.put(holder, pair);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("---MAPPER OUTPUT---");
        for (java.util.Map.Entry<String, Pair<Double, Double>> e : associativeArray.entrySet()) {
            DoubleWritable key = new DoubleWritable(e.getValue().getKey());
            DoubleWritable val = new DoubleWritable(e.getValue().getValue());

            MapWritable value = new MapWritable();
            value.put(key, val);

            context.write(new Text(e.getKey()), value);
        }
        super.cleanup(context);
    }
}
