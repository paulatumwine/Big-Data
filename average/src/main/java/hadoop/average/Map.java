package hadoop.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(Map.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");

        String bytes = words[words.length - 1];
        if (!bytes.matches("\\d+")) return;
        DoubleWritable val = new DoubleWritable(Double.parseDouble(bytes));

        Text word = new Text(words[0]);

        // LogFormat "%h %l %u %t \"%r\" %>s %b" common
        // key - Host/IP Address & value - bytes
        context.write(word, val);
        logger.info("(" + word + ", " + val + ")");
    }
}
