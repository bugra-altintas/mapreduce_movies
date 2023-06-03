import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.*;


public class AverageTask {

    public static class AverageReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }

            float average = sum / count;

            output.collect(key, new FloatWritable(average));

        }


    }
}
