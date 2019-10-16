package hadoop.relative.frequencies.stripes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Map extends Mapper<LongWritable, Text, Text, CustomMapWritable> {

    private Logger logger = Logger.getLogger(Map.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> words = Arrays.asList(value.toString().split("\\s"));

        int size = words.size();
        for (int i = 0; i < size; i++) {
            String u = words.get(i);
            CustomMapWritable stripe = new CustomMapWritable();
            for (String v : window(u, words.subList(i, size))) {
                if (stripe.containsKey(v)) {
                    Double val = Double.parseDouble(stripe.get(v).toString()) + 1D;
                    stripe.put(new Text(v), new DoubleWritable(val));
                } else stripe.put(new Text(v), new DoubleWritable(1D));
            }
            Text k = new Text(u);
            context.write(k, stripe);
            logger.info("(" + k + ", " + stripe + ")");
        }
    }

    List<String> window(String u, List<String> words) {
        List<String> window = new ArrayList<String>();
        for (int i = 0; i < words.size(); i++) {
            if (words.get(i).equals(u)) return window;
            else window.add(words.get(i));
        }
        return window;
    }
}
