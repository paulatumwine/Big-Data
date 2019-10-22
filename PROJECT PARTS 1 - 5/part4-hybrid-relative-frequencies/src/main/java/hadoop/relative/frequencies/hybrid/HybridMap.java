package hadoop.relative.frequencies.hybrid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HybridMap extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

    private Logger logger = Logger.getLogger(HybridMap.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable one = new IntWritable(1);
        List<String> words = Arrays.asList(value.toString().split("\\s+"));
        int size = words.size();
        for (int i = 0; i < size; i++) {
            Text u = new Text(words.get(i));
            for (String v : window(words.get(i), words.subList(i, size))) {
                PairWritable p1 = new PairWritable(u, new Text(v));
                context.write(p1, one);
                logger.info("(" + p1 + ", " + one + ")");
            }
        }
    }

    private List<String> window(String u, List<String> words) {
        List<String> window = new ArrayList<String>();
        for (int i = 1; i < words.size(); i++) {
            if (words.get(i).equals(u)) return window;
            else window.add(words.get(i));
        }
        return window;
    }
}
