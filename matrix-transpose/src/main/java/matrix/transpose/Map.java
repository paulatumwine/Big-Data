package matrix.transpose;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Map extends Mapper<LongWritable, Text, IntWritable, CustomMapWritable> {

    private Logger logger = Logger.getLogger(Map.class);

    public void map(LongWritable id, Text value, Context context) throws IOException, InterruptedException {
        List<String> parts = Arrays.asList(value.toString().split(","));
        // logger.info("(" + parts + ")");
        IntWritable key = new IntWritable(Integer.parseInt(parts.get(0).replaceAll(",|\\s+", "")));

        List<String> words = Arrays.asList(parts.get(1).strip().split("\\s+"));
        int size = words.size();
        for (int column = 0; column < size; column++) {
            CustomMapWritable val = new CustomMapWritable();
            IntWritable v = new IntWritable(Integer.parseInt(words.get(column).strip().replaceAll("\\s+", "")));
            val.put(new IntWritable(Integer.parseInt(key.toString())), v); // use row number as key to map
            logger.info("(" + key + ", " + val + ")");
            context.write(new IntWritable(column), val);
        }
    }
}
