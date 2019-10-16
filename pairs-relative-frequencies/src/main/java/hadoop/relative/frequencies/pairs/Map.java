package hadoop.relative.frequencies.pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Map extends Mapper<LongWritable, Text, StringPairWritable, DoubleWritable> {

    private Logger logger = Logger.getLogger(Map.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DoubleWritable one = new DoubleWritable(1);

        List<String> words = Arrays.asList(value.toString().split("\\s"));

        int size = words.size();
        for (int i = 0; i < size; i++) {
            String u = words.get(i);
            for (String v : window(u, words.subList(i, size))) {
                StringPairWritable p1 = new StringPairWritable(u, v);
                context.write(p1, one);
                logger.info("(" + p1 + ", " + one + ")");
                StringPairWritable p2 = new StringPairWritable(u, "*");
                context.write(p2, one);
                logger.info("(" + p2 + ", " + one + ")");
            }
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
