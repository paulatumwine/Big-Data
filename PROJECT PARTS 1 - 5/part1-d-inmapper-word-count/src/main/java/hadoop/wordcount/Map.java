package hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = Logger.getLogger(Map.class);
    private java.util.Map<String, Integer> associativeArray;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        associativeArray = new HashMap<String, Integer>();
        logger.info("initialized mapper storage successfully");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            word = word.replace(".", "").replace(",", "");
            int count = associativeArray.containsKey(word) ? associativeArray.get(word) + 1 : 1;
            associativeArray.put(word, count);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("---MAPPER OUTPUT---");
        for (java.util.Map.Entry<String, Integer> e: associativeArray.entrySet()) {
            logger.info("(" + e.getKey() + ", " + e.getValue() + ")");
            Text key = new Text(e.getKey());
            IntWritable value = new IntWritable(e.getValue());
            context.write(key, value);
        }
        super.cleanup(context);
    }
}
